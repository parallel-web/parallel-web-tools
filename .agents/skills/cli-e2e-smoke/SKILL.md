---
name: cli-e2e-smoke
description: Use when verifying parallel-cli changes end to end, checking installed CLI behavior, or rerunning offline or live CLI smoke cases in the development loop.
---

# CLI end-to-end smoke checks

After changing CLI behavior or its API integrations, run relevant unit tests and
then exercise the installed CLI from the repository root with this runner. Use
offline checks first; use live checks when hosted API behavior needs verification.
This skill is for the manual dev loop only: do not add live checks to CI, nightly,
or release workflows. The existing offline CI check is unchanged.

## Offline

With development dependencies installed (`uv sync --all-extras`):

```bash
uv run --locked python scripts/cli_smoke.py
uv run --locked python scripts/cli_smoke.py --case 'research poll'
```

`--case` is repeatable. Offline command names and fixtures are in
`scripts/cli_smoke_cases.py`. The loopback API checks requests, authentication,
and output without real credentials; a full run also checks registered help paths.

## Live

Inject a dedicated test account's `PARALLEL_API_KEY` into the process environment
through the available secret mechanism. Never use customer credentials, print the
key, or save it in files. If no test key is available, report live checks unverified.

```bash
uv run --locked python scripts/cli_smoke.py --live
uv run --locked python scripts/cli_smoke.py --live --case enrich
uv run --locked python scripts/cli_smoke.py --live --expensive
```

Live `--case` names are `web`, `research`, `enrich`, `findall`, `monitor`, `memory`,
`skills`, and `local`; repeat the flag to select multiple cases. Dependencies share
a newly created run within each case.

Default live runs use cheap cases (lite processors/read-only calls). Each case's
cost tier and source paths are declared in `scripts/cli_smoke_live.py`. Expensive
FindAll runs only when those paths changed against the merge-base with `origin/main`,
including uncommitted/untracked changes. Ensure `origin/main` is available locally.
Explicit `--case` selection overrides that case's gate; `--expensive` forces all
cases. These calls bill usage, so do not force expensive cases unnecessarily.

See `LIVE_CASES`, `LIVE_SPECS`, and `EXCLUDED_FROM_LIVE` in
`scripts/cli_smoke_live.py` for authoritative coverage and exclusion reasons.
Coverage includes web, research/enrichment/discovery task lifecycles, read-only
monitor/memory access, skills installation, and local CLI utilities. Destructive
operations, top-ups, login, deployment, persistent monitor mutations, and updates
are excluded. Balance get needs an OAuth grant unavailable to the isolated API-key
session; config view/get is standalone-only.

HOME, user config/cache, and skill installation paths are redirected to temporary
directories; neither mode uses saved CLI credentials. Live task runs still create
billable server-side work. The default API is `https://api.parallel.ai`; use
`PARALLEL_API_URL` only for the matching test environment.

## Report results

Record commands, output, and live wall time. Distinguish successful assertions from
skips: unavailable endpoints (403/404), absent monitors, and standalone-only config
are not passes. Polls are bounded to two minutes per run; a FindAll timeout reports
incomplete polling and still checks partial result/schema. Report failures rather
than weakening assertions or retrying costly tasks repeatedly.
