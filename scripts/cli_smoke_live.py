"""Opt-in, bounded live CLI checks. No workflows should invoke this suite."""

import subprocess
from dataclasses import dataclass
from pathlib import Path

from cli_smoke import command_paths, invoke_cli, json_result, live_research, live_web
from cli_smoke_cases import enrich_cases, findall_cases

LIVE_CASES = {
    "web": {"search", "extract", "fetch"},
    "research": {"research processors", "research run", "research status", "research poll"},
    "enrich": {"enrich suggest", "enrich plan", "enrich run", "enrich status", "enrich poll"},
    "findall": {
        "findall ingest",
        "findall entity-search",
        "findall run",
        "findall status",
        "findall poll",
        "findall result",
        "findall schema",
    },
    "monitor": {"monitor list", "monitor get", "monitor events"},
    "memory": {"memory retrieve"},
    "skills": {"skills list", "skills install"},
    "local": {"auth", "completion show", "completion install", "config"},
}
EXCLUDED_FROM_LIVE = {
    "balance get": "Requires a stored OAuth control-API grant; an API key is insufficient (login is excluded).",
    "balance add": "Financial mutation/top-up.",
    "login": "Interactive authentication and credential provisioning.",
    "logout": "Credential deletion.",
    "update": "Changes the installed CLI.",
    "enrich deploy": "Deploys infrastructure.",
    "memory clear": "Deletes memory.",
    "memory evict": "Deletes memory.",
    "monitor create": "Creates persistent resources.",
    "monitor update": "Mutates persistent resources.",
    "monitor trigger": "Triggers persistent resources.",
    "monitor cancel": "Irreversible cancellation.",
    "findall cancel": "Cancels a run.",
    "findall extend": "Adds work to a run.",
    "findall enrich": "Adds work to a run.",
    "skills uninstall": "Removes installed skills.",
    "skills reinstall": "Replaces installed skills.",
}


@dataclass(frozen=True)
class LiveCase:
    commands: set[str]
    cost: str
    sources: tuple[str, ...]


def live_spec(name: str, cost: str, *sources: str) -> LiveCase:
    return LiveCase(LIVE_CASES[name], cost, ("parallel_web_tools/cli/commands.py", *sources))


LIVE_SPECS = {
    "web": live_spec("web", "cheap"),
    "research": live_spec("research", "cheap", "parallel_web_tools/core/research.py"),
    "enrich": live_spec(
        "enrich",
        "cheap",
        "parallel_web_tools/core/batch.py",
        "parallel_web_tools/core/runner.py",
        "parallel_web_tools/cli/planner.py",
    ),
    "findall": live_spec("findall", "expensive", "parallel_web_tools/core/findall.py"),
    "monitor": live_spec("monitor", "cheap", "parallel_web_tools/core/monitor.py"),
    "memory": live_spec("memory", "cheap", "parallel_web_tools/core/memory.py"),
    "skills": live_spec("skills", "cheap", "parallel_web_tools/core/skills.py", "parallel_web_tools/cli/skills.py"),
    "local": live_spec("local", "cheap", "parallel_web_tools/core/auth.py", "parallel_web_tools/cli/updater.py"),
}


def changed_sources(repo: Path) -> set[str]:
    def git(*args: str) -> str:
        result = subprocess.run(("git", *args), cwd=repo, capture_output=True, text=True, check=True)
        return result.stdout.strip()

    try:
        base = git("merge-base", "HEAD", "origin/main")
        return set(git("diff", "--name-only", base, "--").splitlines()) | set(
            git("ls-files", "--others", "--exclude-standard").splitlines()
        )
    except subprocess.CalledProcessError as exc:
        raise SystemExit(
            "Cannot determine live cost gate against origin/main; fetch origin/main or use --expensive."
        ) from exc


def eligible(spec: LiveCase, changed: set[str], *, forced: bool = False) -> bool:
    return forced or spec.cost == "cheap" or bool(changed.intersection(spec.sources))


def require(valid: object, label: str) -> None:
    if not valid:
        raise SystemExit(f"{label}: unexpected live response shape")


def run_live(cli: Path, env: dict[str, str], cwd: Path, selected: list[str] | None, *, expensive: bool = False) -> None:
    from parallel_web_tools.cli import main

    registered = {" ".join(path) for path, group in command_paths(main) if not group}
    require(LIVE_SPECS.keys() == LIVE_CASES.keys(), "live case metadata")
    covered = set().union(*(spec.commands for spec in LIVE_SPECS.values()))
    require(not (covered & EXCLUDED_FROM_LIVE.keys()), "overlapping live classification")
    require(registered == covered | EXCLUDED_FROM_LIVE.keys(), "registered live command coverage")
    unknown = set(selected or ()) - LIVE_CASES.keys()
    if unknown:
        raise SystemExit(f"Unknown live cases: {sorted(unknown)}; choose from {sorted(LIVE_CASES)}")
    env = {
        **env,
        "HOME": str(cwd),
        "USERPROFILE": str(cwd),
        "XDG_CONFIG_HOME": str(cwd / ".config"),
        "XDG_CACHE_HOME": str(cwd / ".cache"),
        "PARALLEL_SKILLS_GLOBAL_DIR": str(cwd / ".agents/skills"),
        "CLAUDE_CONFIG_DIR": str(cwd / ".claude"),
    }

    def run(args: tuple[str, ...], *, raw: bool = False):
        stdout = invoke_cli(cli, args, env, cwd, timeout=135)
        return stdout if raw else json_result(stdout, args)

    for name, reason in EXCLUDED_FROM_LIVE.items():
        print(f"SKIP {name}: {reason}", flush=True)
    changed = set() if expensive or selected else changed_sources(Path(__file__).resolve().parents[1])
    for name in selected or LIVE_SPECS:
        spec = LIVE_SPECS[name]
        if not eligible(spec, changed, forced=expensive or bool(selected)):
            print(
                f"SKIP expensive {name}: no changes since merge-base with origin/main in {', '.join(spec.sources)}; use --case {name} or --expensive",
                flush=True,
            )
            continue
        print(f"RUN {name} (cost: {spec.cost})", flush=True)
        try:
            if name == "web":
                live_web(cli, env, cwd)
            elif name == "research":
                require(run(("research", "processors", "--json")).get("processors"), name)
                live_research(cli, env, cwd)
            elif name == "local":
                require(run(("auth", "--json")).get("authenticated"), "auth")
                for args in (("completion", "show", "--shell", "bash"), ("completion", "install", "--shell", "bash")):
                    require(run(args, raw=True).strip(), "completion")
                require((cwd / ".bashrc").stat().st_size, "completion install")
                for args in (("config", "--json"), ("config", "auto-update-check", "--json")):
                    result = run(args)
                    if result.get("error", {}).get("type") == "UnsupportedOperation":
                        print("SKIP config view/get: standalone CLI only", flush=True)
                    else:
                        require(isinstance(result.get("auto-update-check"), bool), "config")
            elif name == "memory":
                require(
                    isinstance(
                        run(("memory", "retrieve", "IANA", "--scope-key", "cli-smoke", "--limit", "1", "--json")).get(
                            "results"
                        ),
                        list,
                    ),
                    name,
                )
            elif name == "skills":
                skills = run(("skills", "list", "--json"))
                require(skills.get("skills"), "skills list")
                result = run(("skills", "install", "--skill", skills["skills"][0], "--json"))
                require(result.get("file_count", 0) > 0 and result.get("installed_skills"), name)
                require(any((cwd / ".agents/skills").rglob("SKILL.md")), "skills files")
            elif name == "monitor":
                monitors = run(("monitor", "list", "--limit", "1", "--json")).get("monitors")
                require(isinstance(monitors, list), name)
                if not monitors:
                    print("SKIP monitor get/events: no monitors in test account", flush=True)
                else:
                    mid = monitors[0]["monitor_id"]
                    require(mid and run(("monitor", "get", mid, "--json")).get("monitor_id") == mid, name)
                    require(
                        isinstance(run(("monitor", "events", mid, "--limit", "1", "--json")).get("events"), list), name
                    )
            elif name == "enrich":
                fixtures = {case.name: case for case in enrich_cases()}
                require(run(fixtures["enrich suggest"].args).get("enriched_columns"), "enrich suggest")
                plan = fixtures["enrich plan"]
                for path, contents in plan.files.items():
                    (cwd / path).write_text(contents)
                require("Configuration saved" in run((*plan.args, "--processor", "lite"), raw=True), "enrich plan")
                require((cwd / "config.yaml").stat().st_size, "enrich plan file")
                created = run((*fixtures["enrich run"].args, "--processor", "lite"))
                gid = created.get("taskgroup_id")
                require(isinstance(gid, str) and gid.startswith("tgrp_"), "enrich run")
                status = run(("enrich", "status", gid, "--json"))
                require(status.get("taskgroup_id") == gid and status.get("num_runs") == 1, "enrich status")
                result = run(("enrich", "poll", gid, "--timeout", "120", "--poll-interval", "2", "--json"))
                require(isinstance(result, list) and len(result) == 1 and result[0].get("output"), "enrich poll")
            elif name == "findall":
                fixtures = {case.name: case for case in findall_cases()}
                require(run(fixtures["findall ingest"].args).get("entity_type"), "findall ingest")
                entities = run((*fixtures["findall entity-search"].args, "--match-limit", "5"))
                require(isinstance(entities.get("entities"), list), "findall entity-search")
                created = run(fixtures["findall run"].args)
                fid = created.get("findall_id")
                require(isinstance(fid, str) and fid, "findall run")
                require(run(("findall", "status", fid, "--json")).get("status"), "findall status")
                for command in ("poll", "result", "schema"):
                    extra = ("--timeout", "120", "--poll-interval", "2") if command == "poll" else ()
                    try:
                        result = run(("findall", command, fid, "--json", *extra))
                    except SystemExit as exc:
                        if command != "poll" or '"type": "TimeoutError"' not in str(exc):
                            raise
                        print(
                            f"SKIP findall poll completion: {fid} still active after 120s; checking partial result/schema",
                            flush=True,
                        )
                        continue
                    if command == "schema":
                        require(result.get("entity_type") and result.get("match_conditions"), "findall schema")
                    else:
                        require(
                            result.get("findall_id") == fid
                            and result.get("status") in {"completed", "running", "queued", "pending"}
                            and isinstance(result.get("candidates"), list)
                            and (result.get("status") != "completed" or result["candidates"]),
                            f"findall {command}",
                        )
            print(f"parallel-cli {name} checks finished (live case; skips reported above).", flush=True)
        except SystemExit as exc:
            message = str(exc)
            if any(code in message for code in ("403", "404", "UnsupportedOperation")):
                print(f"SKIP remaining {name} commands: endpoint unavailable: {message}", flush=True)
            else:
                raise
