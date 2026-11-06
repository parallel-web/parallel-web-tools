"""Send feedback about Parallel to the Parallel team."""

from __future__ import annotations

import re
from typing import Any, Literal

from parallel_web_tools.core.auth import create_client
from parallel_web_tools.core.user_agent import ClientSource

FeedbackCategory = Literal["results_quality", "error", "latency", "missing_capability", "docs", "billing", "other"]
FeedbackProduct = Literal[
    "search", "extract", "task", "findall", "monitor", "responses", "decisions", "mcp", "cli", "other"
]
FeedbackSeverity = Literal["blocking", "degraded", "minor"]

FEEDBACK_CATEGORIES: tuple[FeedbackCategory, ...] = (
    "results_quality",
    "error",
    "latency",
    "missing_capability",
    "docs",
    "billing",
    "other",
)
FEEDBACK_PRODUCTS: tuple[FeedbackProduct, ...] = (
    "search",
    "extract",
    "task",
    "findall",
    "monitor",
    "responses",
    "decisions",
    "mcp",
    "cli",
    "other",
)
FEEDBACK_SEVERITIES: tuple[FeedbackSeverity, ...] = ("blocking", "degraded", "minor")

MIN_MESSAGE_CHARS = 20
MAX_MESSAGE_CHARS = 4000
MAX_EXAMPLES = 50
MAX_NOTE_CHARS = 1000
_EXAMPLE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")

_FEEDBACK_PATH = "/v1beta/feedback"


class FeedbackInputError(ValueError):
    """Raised when a feedback request is invalid before it reaches the API."""


def parse_example(raw: str) -> dict[str, str]:
    """Parse ``ID`` or ``ID:note`` into an example; IDs never contain a colon."""
    example_id, sep, note = raw.partition(":")
    example_id = example_id.strip()
    if not _EXAMPLE_ID_RE.fullmatch(example_id):
        raise FeedbackInputError(
            f"example {raw!r} must start with a Parallel ID such as search_..., extract_..., or trun_..."
        )
    note = note.strip()
    if len(note) > MAX_NOTE_CHARS:
        raise FeedbackInputError(f"example note must be at most {MAX_NOTE_CHARS} characters")
    return {"id": example_id, "issue": note} if sep and note else {"id": example_id}


def _check_choice(value: str, choices: tuple[str, ...], field: str) -> None:
    if value not in choices:
        raise FeedbackInputError(f"{field} must be one of: {', '.join(choices)}")


def send_feedback(
    message: str,
    category: str,
    product: str,
    *,
    severity: str = "degraded",
    examples: list[dict[str, str]] | None = None,
    expected: str | None = None,
    actual: str | None = None,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Send a feedback report; the response shows exactly what Parallel received."""
    if not MIN_MESSAGE_CHARS <= len(message) <= MAX_MESSAGE_CHARS:
        raise FeedbackInputError(f"message must be {MIN_MESSAGE_CHARS}-{MAX_MESSAGE_CHARS} characters")
    _check_choice(category, FEEDBACK_CATEGORIES, "category")
    _check_choice(product, FEEDBACK_PRODUCTS, "product")
    _check_choice(severity, FEEDBACK_SEVERITIES, "severity")
    if examples is not None and len(examples) > MAX_EXAMPLES:
        raise FeedbackInputError(f"examples must be at most {MAX_EXAMPLES}")

    body: dict[str, Any] = {
        "message": message,
        "category": category,
        "product": product,
        "severity": severity,
    }
    if examples:
        body["examples"] = examples
    if expected:
        body["expected"] = expected
    if actual:
        body["actual"] = actual

    client = create_client(api_key, source)
    return client.post(path=_FEEDBACK_PATH, body=body, cast_to=dict)
