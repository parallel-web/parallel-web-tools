# AGENTS.md

Guidance for coding agents working in this repository.

## Recommended iteration cycle after code changes

Run commands from the repository root.

### 1) Autofix lint issues first

```bash
uv run ruff check --fix .
```

### 2) Format code

```bash
uv run ruff format .
```

### 3) Run architecture checks

```bash
uv run tach check
```

If `tach check` fails, do not immediately restructure modules or change dependency boundaries on your own. Confirm with the user before making architectural fixes, since Tach failures can imply intended or unintended module boundary changes.

### 4) Run type checks

```bash
uv run ty check
```

### 5) Run tests

For targeted changes, run the relevant test files first:

```bash
uv run pytest -q tests/test_cli.py tests/test_skills.py
```

For broader changes or before finishing, run the full test suite:

```bash
uv run pytest -q
```

## Expectations

- Prefer `uv run ruff check --fix .` before manual cleanup.
- Run `uv run tach check` after code changes that may affect imports or module boundaries.
- If `uv run tach check` fails, confirm with the user before making architectural/module-layout fixes.
- Run targeted tests during iteration for faster feedback.
- Run the full relevant validation before finishing work.
- If a command fails, fix the reported issues before finishing.
