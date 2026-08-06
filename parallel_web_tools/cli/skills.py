"""Skills CLI commands for parallel-cli."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, NoReturn, Protocol

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


def _project_root_for(install_dir: str, project: bool) -> Path | None:
    """Return the project root a project-local install dir sits in.

    Project installs land in ``<root>/.agents/skills``; global installs have no
    project root, so Claude Code's global tree is used instead.
    """
    if not project:
        return None
    return Path(install_dir).parent.parent


def _print_skill_commands(console: Console, skill_names: list[str]) -> None:
    """Print how to invoke the installed skills.

    Skills installed by the CLI are loose (not bundled in a Claude Code plugin),
    so their slash command is the un-namespaced folder name.
    """
    if not skill_names:
        return
    commands = ", ".join(f"/{name}" for name in skill_names)
    console.print(f"Skill commands: [cyan]{commands}[/cyan]")


def _print_claude_code_result(console: Console, claude_code: dict[str, Any]) -> None:
    if claude_code["linked"] or claude_code["copied"]:
        console.print(
            f"Linked into Claude Code ([cyan]{claude_code['claude_skills_dir']}[/cyan]); "
            "restart Claude Code to pick up the new skills."
        )
    for warning in claude_code["warnings"]:
        console.print(f"[yellow]{warning}[/yellow]")


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
            link_into_claude_code,
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

        # Make the freshly installed skills visible to Claude Code (best-effort).
        result["claude_code"] = link_into_claude_code(
            Path(str(result["install_dir"])),
            list(result["installed_skills"]),
            project_root=_project_root_for(str(result["install_dir"]), project),
        )

        if output_json:
            print(json.dumps(result, indent=2))
            return

        console.print("[bold green]Skills installed[/bold green]")
        console.print(f"Location: [cyan]{result['install_dir']}[/cyan]")
        console.print(f"Ref: [cyan]{result['ref']}[/cyan]")
        console.print(f"Installed ({result['count']}): [cyan]{', '.join(result['installed_skills'])}[/cyan]")
        _print_skill_commands(console, list(result["installed_skills"]))
        _print_claude_code_result(console, result["claude_code"])

    @skills.command(name="uninstall")
    @click.option(
        "--project",
        is_flag=True,
        help="Uninstall from .agents/skills in detected project root (default is global install).",
    )
    @click.option("--json", "output_json", is_flag=True, help="Output as JSON")
    def skills_uninstall(project: bool, output_json: bool) -> None:
        """Uninstall skills previously installed by parallel-cli."""
        from parallel_web_tools.core.skills import (
            SkillsInstallLocationError,
            resolve_install_dir,
            uninstall_skills,
            unlink_from_claude_code,
        )

        try:
            install_dir = resolve_install_dir(project=project)
            result = uninstall_skills(install_dir=install_dir)
        except SkillsInstallLocationError as e:
            handle_error(e, output_json=output_json, exit_code=exit_bad_input, prefix="Skills uninstall failed")
        except Exception as e:
            handle_error(e, output_json=output_json, exit_code=exit_api_error, prefix="Skills uninstall failed")

        # Drop the Claude Code links pointing at the skills we just removed.
        result["claude_code"] = unlink_from_claude_code(
            Path(str(result["install_dir"])),
            list(result["removed_skills"]),
            project_root=_project_root_for(str(result["install_dir"]), project),
        )

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
        for warning in result["claude_code"]["warnings"]:
            console.print(f"[yellow]{warning}[/yellow]")

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
            link_into_claude_code,
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

        result["claude_code"] = link_into_claude_code(
            Path(str(result["install_dir"])),
            list(result["installed_skills"]),
            project_root=_project_root_for(str(result["install_dir"]), project),
        )

        if output_json:
            print(json.dumps(result, indent=2))
            return

        console.print("[bold green]Skills reinstalled[/bold green]")
        console.print(f"Location: [cyan]{result['install_dir']}[/cyan]")
        console.print(f"Ref: [cyan]{result['ref']}[/cyan]")
        console.print(f"Removed ({result['removed_count']}): [cyan]{', '.join(result['removed_skills'])}[/cyan]")
        console.print(f"Installed ({result['installed_count']}): [cyan]{', '.join(result['installed_skills'])}[/cyan]")
        _print_skill_commands(console, list(result["installed_skills"]))
        _print_claude_code_result(console, result["claude_code"])

    return skills
