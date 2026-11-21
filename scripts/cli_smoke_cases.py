"""Offline request/response fixtures for the CLI's supported API commands."""

import json
from dataclasses import dataclass, field

FIXTURE_KEY = "cli-smoke-fixture"
API_KEY = ("x-api-key", FIXTURE_KEY)
CONTROL_TOKEN = "cli-smoke-control"
BEARER = ("Authorization", f"Bearer {CONTROL_TOKEN}")
URL = "https://example.com/"
QUERY = "IANA example domains"
CONTENT = "Example Domain (CLI smoke fixture)"
WEB_ARGS = {
    "search": ("--query", QUERY, "--mode", "fast", "--max-results", "1"),
    "extract": (URL, "--full-content"),
    "fetch": (URL, "--full-content"),
}
RESEARCH_QUERY = "What is the purpose of the example.com domain? Answer in one sentence."
RESEARCH_PROCESSOR = "lite"
RESEARCH_ARGS = ("research", "run", RESEARCH_QUERY, "--processor", RESEARCH_PROCESSOR, "--no-wait", "--json")
EXCLUDED_COMMANDS = {
    ("enrich", "deploy"),
    ("memory", "clear"),
    ("memory", "evict"),
    ("monitor", "cancel"),
    ("skills", "uninstall"),
    ("logout",),
}
HELP_ONLY_COMMANDS = {
    ("auth",),
    ("completion", "install"),
    ("completion", "show"),
    ("config",),
    ("research", "processors"),
    ("update",),
}


@dataclass(frozen=True)
class Exchange:
    method: str
    path: str
    reply: object = None
    body: dict | None = None
    auth: tuple[str, str] | None = API_KEY
    query: dict | None = None
    status: int = 200
    content_type: str | None = None


@dataclass(frozen=True)
class Case:
    name: str
    args: tuple[str, ...]
    exchanges: tuple[Exchange, ...]
    expected: dict | list[dict] | str
    files: dict[str, str] = field(default_factory=dict)
    json_lines: bool = False
    output_file: str | None = None


def research_poll_args(run_id: str, *, interval: int = 2) -> tuple[str, ...]:
    return (
        "research",
        "poll",
        run_id,
        "--timeout",
        "120",
        "--poll-interval",
        str(interval),
        "--output",
        "research-result",
        "--json",
    )


def web_cases() -> tuple[Case, ...]:
    result = []
    for command, args in WEB_ARGS.items():
        route = "search" if command == "search" else "extract"
        page = {"url": URL, "title": "Example Domain", "excerpts": [CONTENT]}
        if route == "extract":
            page["full_content"] = CONTENT
        reply = {f"{route}_id": f"{route}_smoke", "session_id": "session_smoke", "results": [page], "errors": []}
        body = (
            {"search_queries": [QUERY], "mode": "fast", "advanced_settings": {"max_results": 1}}
            if route == "search"
            else {"urls": [URL], "advanced_settings": {"full_content": True}}
        )
        expected = {"status": "ok", f"{route}_id": f"{route}_smoke", "results": [{**page, "publish_date": None}]}
        result.append(
            Case(command, (command, *args, "--json"), (Exchange("POST", f"/v1/{route}", reply, body),), expected)
        )
    return tuple(result)


def research_cases() -> tuple[Case, ...]:
    run_id = "trun_smoke"
    path = f"/v1/tasks/runs/{run_id}"
    active = {"run_id": run_id, "status": "running", "interaction_id": "interaction_smoke"}
    completed = {**active, "status": "completed"}
    output = {"type": "json", "content": {"purpose": CONTENT}, "basis": []}
    return (
        Case(
            "research run",
            RESEARCH_ARGS,
            (Exchange("POST", "/v1/tasks/runs", active, {"input": RESEARCH_QUERY, "processor": RESEARCH_PROCESSOR}),),
            {"run_id": run_id, "processor": RESEARCH_PROCESSOR, "status": "running"},
        ),
        Case(
            "research status",
            ("research", "status", run_id, "--json"),
            (Exchange("GET", path, active),),
            {"run_id": run_id, "status": "running"},
        ),
        Case(
            "research poll",
            research_poll_args(run_id, interval=0),
            (
                Exchange("GET", path, active),
                Exchange("GET", path, completed),
                Exchange("GET", f"{path}/result", {"run": completed, "output": output}),
            ),
            {"run_id": run_id, "status": "completed", "output": {"content": {"purpose": CONTENT}}},
            output_file="research-result.json",
        ),
    )


def account_cases(endpoint: str) -> tuple[Case, ...]:
    org, app, client = "org_smoke", "app_smoke", "client_smoke"
    tokens = {
        "org_id": org,
        "access_token": CONTROL_TOKEN,
        "refresh_token": "refresh_smoke",
        "expires_in": 3600,
        "refresh_token_expires_in": 3600,
        "authorization_expires_in": 3600,
    }
    app_name = "parallel-cli Users"
    key = {
        "api_key_id": "key_smoke",
        "api_key_name": "E2E key",
        "raw_api_key": FIXTURE_KEY,
        "display_value": "smoke",
        "app_id": app,
        "app_name": app_name,
        "created_by_user_id": "user_smoke",
        "created_by_user_email": "fixture@example.com",
        "created_at": 1,
        "last_used_at": 1,
        "updated_at": 1,
    }
    login = Case(
        "login",
        ("login", "--no-browser", "--json"),
        (
            Exchange("POST", "/getServiceKeys/register", {"client_id": client}, {"client_name": "parallel-cli"}, None),
            Exchange(
                "POST",
                "/getServiceKeys/device/code",
                {"device_code": "device_smoke", "user_code": "SMOKE", "verification_uri": f"{endpoint}/verify"},
                {"client_id": [client]},
                None,
            ),
            Exchange(
                "POST", "/getServiceKeys/token", tokens, {"client_id": [client], "device_code": ["device_smoke"]}, None
            ),
            Exchange(
                "GET",
                "/service/v1/apps",
                {"apps": [{"org_id": org, "app_id": app, "app_name": app_name, "org_name": "Fixture"}]},
                auth=BEARER,
            ),
            Exchange("POST", f"/service/v1/apps/{app}/keys", key, auth=BEARER),
        ),
        [
            {"event": "auth_start"},
            {"event": "device_code", "user_code": "SMOKE", "browser_opened": False},
            {"event": "auth_waiting"},
            {"event": "auth_success"},
        ],
        json_lines=True,
        output_file=".config/parallel-web-tools/auth.json",
    )
    auth_state = {
        "version": 1,
        "selected_org_id": org,
        "orgs": {
            org: {
                "control_api": {
                    "access_token": CONTROL_TOKEN,
                    "access_token_expires_at": 4102444800,
                }
            }
        },
    }
    files = {".config/parallel-web-tools/auth.json": json.dumps(auth_state)}
    balance = {"org_id": org, "credit_balance_cents": 42}
    topped_up = {"org_id": org, "credit_balance_cents": 142}
    return (
        login,
        Case(
            "balance get",
            ("balance", "--json", "get"),
            (Exchange("GET", "/service/v1/balance", balance, auth=BEARER),),
            balance,
            files=files,
        ),
        Case(
            "balance add",
            ("balance", "--json", "add", "100", "--idempotency-key", "fixture-top-up"),
            (
                Exchange(
                    "POST",
                    "/service/v1/balance/add",
                    topped_up,
                    {"amount_cents": 100, "idempotency_key": "fixture-top-up"},
                    BEARER,
                ),
            ),
            topped_up,
            files=files,
        ),
    )


def memory_cases() -> tuple[Case, ...]:
    response = {"results": [], "next_cursor": None}
    return (
        Case(
            "memory retrieve",
            ("memory", "retrieve", QUERY, "--limit", "1", "--json"),
            (Exchange("POST", "/v1beta/memory/retrieve", response, {"query": QUERY, "limit": 1}),),
            {"results": []},
        ),
    )


def enrich_cases() -> tuple[Case, ...]:
    intent = "Find the official website"
    input_row = {"company": "Example"}
    output_row = {"website": URL}
    source_columns = '[{"name":"company","description":"The company field"}]'
    columns = [{"name": "website", "type": "str", "description": "Official website"}]
    suggest = {
        "output_schema": {
            "type": "object",
            "properties": {
                "website": {"type": "string", "description": "Official website"},
            },
        }
    }
    processor_suggestion = Exchange(
        "POST",
        "/v1beta/tasks/suggest-processor",
        {"recommended_processors": ["lite"]},
        {"task_spec": {"output_schema": suggest["output_schema"]}},
    )
    group = "tgrp_smoke"
    group_path = f"/v1/tasks/groups/{group}"
    completed = {
        "taskgroup_id": group,
        "status": {
            "is_active": False,
            "num_task_runs": 1,
            "task_run_status_counts": {"completed": 1},
        },
    }
    run_event = {
        "type": "task_run.state",
        "run": {"run_id": "trun_smoke", "status": "completed"},
        "input": {"input": input_row},
        "output": {"type": "json", "content": output_row, "basis": []},
    }
    sse = f"event: task_run.state\ndata: {json.dumps(run_event)}\n\n"
    return (
        Case(
            "enrich suggest",
            ("enrich", "suggest", intent, "--json"),
            (Exchange("POST", "/v1beta/tasks/suggest", suggest, {"user_intent": intent}), processor_suggestion),
            {"enriched_columns": columns, "processor": "lite"},
        ),
        Case(
            "enrich plan",
            (
                "enrich",
                "plan",
                "--source-type",
                "csv",
                "--source",
                "companies.csv",
                "--target",
                "enriched.csv",
                "--source-columns",
                source_columns,
                "--intent",
                intent,
            ),
            (
                Exchange(
                    "POST",
                    "/v1beta/tasks/suggest",
                    suggest,
                    {
                        "user_intent": f"{intent}\n\nInput columns available:\n- company: The company field",
                    },
                ),
                processor_suggestion,
            ),
            "Configuration saved",
            output_file="config.yaml",
            files={"companies.csv": "company\nExample\n"},
        ),
        Case(
            "enrich run",
            (
                "enrich",
                "run",
                "--data",
                json.dumps([input_row]),
                "--target",
                "enriched.csv",
                "--enriched-columns",
                json.dumps(columns),
                "--no-wait",
                "--json",
            ),
            (
                Exchange("POST", "/v1/tasks/groups", completed),
                Exchange(
                    "POST",
                    f"{group_path}/runs",
                    {"run_ids": ["trun_smoke"]},
                    {"inputs": [{"input": input_row, "processor": "core-fast"}]},
                ),
            ),
            {"taskgroup_id": group, "num_runs": 1},
        ),
        Case(
            "enrich status",
            ("enrich", "status", group, "--json"),
            (Exchange("GET", group_path, completed),),
            {"taskgroup_id": group, "num_runs": 1, "is_active": False},
        ),
        Case(
            "enrich poll",
            ("enrich", "poll", group, "--json"),
            (
                Exchange("GET", group_path, completed),
                Exchange(
                    "GET",
                    f"{group_path}/runs",
                    sse,
                    query={"include_input": ["true"], "include_output": ["true"]},
                    content_type="text/event-stream",
                ),
            ),
            [{"input": input_row, "output": output_row}],
        ),
    )


def findall_cases() -> tuple[Case, ...]:
    objective = "Find standards organizations for the public web"
    run_id = "findall_smoke"
    runs = "/v1beta/findall/runs"
    path = f"{runs}/{run_id}"
    schema = {
        "objective": objective,
        "entity_type": "companies",
        "match_conditions": [
            {"name": "standards", "description": "Publishes open standards"},
        ],
    }
    ingest = Exchange("POST", "/v1beta/findall/ingest", schema, {"objective": objective})
    completed = {"findall_id": run_id, "generator": "base", "status": {"status": "completed", "is_active": False}}
    reply = {"run": completed, "candidates": []}
    status = {"findall_id": run_id, "status": "completed"}
    result = {**status, "candidates": []}
    result_request = Exchange("GET", f"{path}/result", reply)
    retrieve = Exchange("GET", path, completed)
    output_schema = {"type": "object", "properties": {"website": {"type": "string"}}}
    entities = {"entities": []}
    return (
        Case("findall ingest", ("findall", "ingest", objective, "--json"), (ingest,), {"entity_type": "companies"}),
        Case(
            "findall run",
            ("findall", "run", objective, "--generator", "base", "--match-limit", "5", "--no-wait", "--json"),
            (ingest, Exchange("POST", runs, completed, {**schema, "generator": "base", "match_limit": 5})),
            status,
        ),
        Case("findall status", ("findall", "status", run_id, "--json"), (retrieve,), status),
        Case("findall poll", ("findall", "poll", run_id, "--json"), (retrieve, result_request), result),
        Case("findall result", ("findall", "result", run_id, "--json"), (result_request,), result),
        Case(
            "findall cancel",
            ("findall", "cancel", run_id, "--json"),
            (Exchange("POST", f"{path}/cancel", status=204),),
            {"findall_id": run_id, "status": "cancelled"},
        ),
        Case(
            "findall entity-search",
            ("findall", "entity-search", objective, "--entity-type", "companies", "--json"),
            (
                Exchange(
                    "POST",
                    "/v1beta/findall/entity-search",
                    entities,
                    {"objective": objective, "entity_type": "companies"},
                ),
            ),
            entities,
        ),
        Case(
            "findall enrich",
            ("findall", "enrich", run_id, json.dumps(output_schema), "--json"),
            (
                Exchange(
                    "POST",
                    f"{path}/enrich",
                    completed,
                    {"output_schema": {"type": "json", "json_schema": output_schema}},
                ),
            ),
            {"findall_id": run_id},
        ),
        Case(
            "findall extend",
            ("findall", "extend", run_id, "5", "--json"),
            (Exchange("POST", f"{path}/extend", completed, {"additional_match_limit": 5}),),
            {"findall_id": run_id},
        ),
        Case(
            "findall schema",
            ("findall", "schema", run_id, "--json"),
            (Exchange("GET", f"{path}/schema", schema),),
            {"entity_type": "companies"},
        ),
    )


def monitor_cases() -> tuple[Case, ...]:
    query = "News from IANA"
    monitor_id = "mon_smoke"
    monitors = "/v1/monitors"
    path = f"{monitors}/{monitor_id}"
    monitor = {"monitor_id": monitor_id, "type": "event_stream", "frequency": "1d", "status": "active"}
    updated = {**monitor, "frequency": "2d"}
    listing = {"monitors": [monitor], "next_cursor": None}
    events = {"events": [], "next_cursor": None}
    return (
        Case(
            "monitor create",
            ("monitor", "create", query, "--json"),
            (
                Exchange(
                    "POST", monitors, monitor, {"frequency": "1d", "type": "event_stream", "settings": {"query": query}}
                ),
            ),
            monitor,
        ),
        Case("monitor list", ("monitor", "list", "--json"), (Exchange("GET", monitors, listing),), listing),
        Case("monitor get", ("monitor", "get", monitor_id, "--json"), (Exchange("GET", path, monitor),), monitor),
        Case(
            "monitor update",
            ("monitor", "update", monitor_id, "--frequency", "2d", "--json"),
            (Exchange("POST", f"{path}/update", updated, {"frequency": "2d"}),),
            updated,
        ),
        Case(
            "monitor events",
            ("monitor", "events", monitor_id, "--limit", "1", "--json"),
            (Exchange("GET", f"{path}/events", events, query={"limit": ["1"]}),),
            events,
        ),
        Case(
            "monitor trigger",
            ("monitor", "trigger", monitor_id, "--json"),
            (Exchange("POST", f"{path}/trigger", status=204),),
            {"monitor_id": monitor_id, "triggered": True},
        ),
    )


def skills_cases(endpoint: str) -> tuple[Case, ...]:
    skill = {"name": "smoke", "skill_url": f"{endpoint}/skills/SKILL.md"}
    index = Exchange("GET", "/skills/index.json", {"skills": [skill]}, auth=None)
    download = Exchange("GET", "/skills/SKILL.md", "---\nname: smoke\n---\nFixture skill\n", auth=None)
    return (
        Case("skills list", ("skills", "list", "--json"), (index, index), {"skills": ["smoke"]}),
        Case(
            "skills install",
            ("skills", "install", "--skill", "smoke", "--json"),
            (index, download),
            {"installed_skills": ["smoke"]},
            output_file=".agents/skills/smoke/SKILL.md",
        ),
        Case(
            "skills reinstall",
            ("skills", "reinstall", "--skill", "smoke", "--json"),
            (index, download),
            {"installed_skills": ["smoke"]},
            output_file=".agents/skills/smoke/SKILL.md",
        ),
    )


def cases(endpoint: str) -> tuple[Case, ...]:
    return (
        *web_cases(),
        *research_cases(),
        *account_cases(endpoint),
        *memory_cases(),
        *enrich_cases(),
        *findall_cases(),
        *monitor_cases(),
        *skills_cases(endpoint),
    )
