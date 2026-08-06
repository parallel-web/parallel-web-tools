"""Skills CLI commands for parallel-cli."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import NoReturn, Protocol

import click
from rich.console import Console


class HandleError(Protocol):
    def __call__(
        self,
        error: Exception,
        output_json: bool = False,
        exit_code: int = 0,
        prefix: str = "Error",
    ) -> NoReturn: ...


def _report_installed(console: Console, install_dir: str, skill_names: list[str], project: bool) -> None:
    """Print how to invoke the installed skills, linking them into Claude Code first.

    Skills install into ``.agents/skills``, which Gemini CLI, Copilot, Codex and
    Amp read but Claude Code does not -- it only scans ``.claude/skills``. Link
    rather than copy so one canonical copy serves every agent.
    """
    install_path = Path(install_dir)

    # These are loose skills, so each is invoked by its folder name.
    console.print(f"Commands: [cyan]{', '.join('/' + name for name in skill_names)}[/cyan]")

    claude_skills = (install_path.parent.parent if project else Path.home()) / ".claude" / "skills"
    if not claude_skills.parent.exists():
        return  # Claude Code is not installed here.

    linked = False
    for name in skill_names:
        link = claude_skills / name
        if link.exists() or link.is_symlink():
            continue  # Never clobber an existing skill, and never relink our own.
        try:
            claude_skills.mkdir(parents=True, exist_ok=True)
            os.symlink(install_path / name, link, target_is_directory=True)
            linked = True
        except OSError as e:
            # Windows needs Developer Mode or admin rights to create symlinks.
            console.print(f"[yellow]Could not link '{name}' into Claude Code: {e}[/yellow]")

    if linked:
        console.print(f"Linked into Claude Code ([cyan]{claude_skills}[/cyan]); restart it to pick them up.")


def create_skills_group(
    console: Console,
    handle_error: HandleError,
    exit_bad_input: int,
    exit_api_error: int,
) -> click.Group:
    """Create the skills command group.

    Keeps feature-specific command wiring out of ``commands.py`` while retaining
    lazy imports of the underlying skills implementation.
    """

    @click.group(name="skills")
    def skills() -> None:
        """Install and manage Parallel agent skills.

        Downloads come from skills.parallel.ai. Set PARALLEL_SKILLS_INDEX_URL to use a custom index.
        """
        pass

    @skills.command(name="list")
    @click.option("--json", "output_json", is_flag=True, help="Output as JSON")
    def skills_list(output_json: bool) -> None:
        """List available Parallel skills from skills.parallel.ai."""
        from parallel_web_tools.core.skills import SkillsError, get_remote_skills_channel, list_remote_skills

        try:
            ref = get_remote_skills_channel()
            skill_names = list_remote_skills()
        except SkillsError as e:
            handle_error(e, output_json=output_json, exit_code=exit_api_error, prefix="Skills list failed")
        except Exception as e:
            handle_error(e, output_json=output_json, exit_code=exit_api_error, prefix="Skills list failed")

        if output_json:
            print(json.dumps({"ref": ref, "skills": skill_names, "count": len(skill_names)}, indent=2))
            return

        console.print("[bold]Available skills[/bold]")
        console.print(f"Ref: [cyan]{ref}[/cyan]")
        for skill_name in skill_names:
            console.print(f"- [cyan]{skill_name}[/cyan]")

    @skills.command(name="install")
    @click.option(
        "--project",
        is_flag=True,
        help="Install to .agents/skills in detected project root (default is global install).",
    )
    @click.option(
        "--skill",
        "skill_names",
        multiple=True,
        help="Skill name to install (repeatable). Defaults to all. Skills not listed will be removed.",
    )
    @click.option("--json", "output_json", is_flag=True, help="Output as JSON")
    def skills_install(project: bool, skill_names: tuple[str, ...], output_json: bool) -> None:
        """Install Parallel skills from skills.parallel.ai.

        When --skill is provided, the managed install set is replaced with exactly
        the listed skills.
        """
        from parallel_web_tools.core.skills import (
            SkillsError,
            SkillsInputError,
            SkillsInstallLocationError,
            install_skills,
            resolve_install_dir,
        )

        try:
            install_dir = resolve_install_dir(project=project)
            result = install_skills(
                install_dir=install_dir,
                selected_skills=list(skill_names) or None,
            )
        except SkillsInstallLocationError as e:
            handle_error(e, output_json=output_json, exit_code=exit_bad_input, prefix="Skills install failed")
        except SkillsInputError as e:
            handle_error(e, output_json=output_json, exit_code=exit_bad_input, prefix="Skills install failed")
        except SkillsError as e:
            handle_error(e, output_json=output_json, exit_code=exit_api_error, prefix="Skills install failed")
        except Exception as e:
            handle_error(e, output_json=output_json, exit_code=exit_api_error, prefix="Skills install failed")

        if output_json:
            print(json.dumps(result, indent=2))
            return

        console.print("[bold green]Skills installed[/bold green]")
        console.print(f"Location: [cyan]{result['install_dir']}[/cyan]")
        console.print(f"Ref: [cyan]{result['ref']}[/cyan]")
        console.print(f"Installed ({result['count']}): [cyan]{', '.join(result['installed_skills'])}[/cyan]")
        _report_installed(console, str(result["install_dir"]), list(result["installed_skills"]), project)

    @skills.command(name="uninstall")
    @click.option(
        "--project",
        is_flag=True,
        help="Uninstall from .agents/skills in detected project root (default is global install).",
    )
    @click.option("--json", "output_json", is_flag=True, help="Output as JSON")
    def skills_uninstall(project: bool, output_json: bool) -> None:
        """Uninstall skills previously installed by parallel-cli."""
        from parallel_web_tools.core.skills import SkillsInstallLocationError, resolve_install_dir, uninstall_skills

        try:
            install_dir = resolve_install_dir(project=project)
            result = uninstall_skills(install_dir=install_dir)
        except SkillsInstallLocationError as e:
            handle_error(e, output_json=output_json, exit_code=exit_bad_input, prefix="Skills uninstall failed")
        except Exception as e:
            handle_error(e, output_json=output_json, exit_code=exit_api_error, prefix="Skills uninstall failed")

        if output_json:
            print(json.dumps(result, indent=2))
            return

        if result["count"] == 0:
            console.print("[yellow]No managed skills found to uninstall[/yellow]")
            console.print(f"Location: [cyan]{result['install_dir']}[/cyan]")
            return

        console.print("[bold green]Skills uninstalled[/bold green]")
        console.print(f"Location: [cyan]{result['install_dir']}[/cyan]")
        console.print(f"Removed ({result['count']}): [cyan]{', '.join(result['removed_skills'])}[/cyan]")

    @skills.command(name="reinstall")
    @click.option(
        "--project",
        is_flag=True,
        help="Reinstall in .agents/skills in detected project root (default is global install).",
    )
    @click.option(
        "--skill",
        "skill_names",
        multiple=True,
        help="Skill name to reinstall (repeatable). Defaults to all. Skills not listed will be removed.",
    )
    @click.option("--json", "output_json", is_flag=True, help="Output as JSON")
    def skills_reinstall(project: bool, skill_names: tuple[str, ...], output_json: bool) -> None:
        """Reinstall Parallel skills (uninstall managed set then install fresh).

        When --skill is provided, the managed install set is replaced with exactly
        the listed skills.
        """
        from parallel_web_tools.core.skills import (
            SkillsError,
            SkillsInputError,
            SkillsInstallLocationError,
            reinstall_skills,
            resolve_install_dir,
        )

        try:
            install_dir = resolve_install_dir(project=project)
            result = reinstall_skills(
                install_dir=install_dir,
                selected_skills=list(skill_names) or None,
            )
        except SkillsInstallLocationError as e:
            handle_error(e, output_json=output_json, exit_code=exit_bad_input, prefix="Skills reinstall failed")
        except SkillsInputError as e:
            handle_error(e, output_json=output_json, exit_code=exit_bad_input, prefix="Skills reinstall failed")
        except SkillsError as e:
            handle_error(e, output_json=output_json, exit_code=exit_api_error, prefix="Skills reinstall failed")
        except Exception as e:
            handle_error(e, output_json=output_json, exit_code=exit_api_error, prefix="Skills reinstall failed")

        if output_json:
            print(json.dumps(result, indent=2))
            return

        console.print("[bold green]Skills reinstalled[/bold green]")
        console.print(f"Location: [cyan]{result['install_dir']}[/cyan]")
        console.print(f"Ref: [cyan]{result['ref']}[/cyan]")
        console.print(f"Removed ({result['removed_count']}): [cyan]{', '.join(result['removed_skills'])}[/cyan]")
        console.print(f"Installed ({result['installed_count']}): [cyan]{', '.join(result['installed_skills'])}[/cyan]")
        _report_installed(console, str(result["install_dir"]), list(result["installed_skills"]), project)

    return skills
