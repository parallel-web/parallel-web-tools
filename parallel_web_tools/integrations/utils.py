"""Shared utilities for integrations."""

from __future__ import annotations

import sys


def confirm_overwrite(existing_resources: list[str]) -> bool:
    """Prompt user to confirm overwriting existing resources.

    Args:
        existing_resources: List of resource descriptions that will be overwritten.

    Returns:
        True if the user confirms, False otherwise.
    """
    print("\nThe following resources already exist and will be overwritten:")
    for resource in existing_resources:
        print(f"  - {resource}")
    print()

    # Check if running in interactive mode
    if not sys.stdin.isatty():
        print("Non-interactive mode detected. Use force=True to skip confirmation.")
        return False

    try:
        response = input("Do you want to continue? [y/N]: ").strip().lower()
        return response in ("y", "yes")
    except (EOFError, KeyboardInterrupt):
        print()
        return False
