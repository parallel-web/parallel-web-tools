"""Skills CLI commands for parallel-cli."""

from __future__ import annotations

import json
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
        """Install, update, and manage Parallel agent skills.

        Run parallel-cli skills install to install or update managed skills after
        updating parallel-cli. Use parallel-cli skills reinstall for a clean reinstall.
        Skills not managed by parallel-cli are left untouched.

        Downloads come from skills.parallel.ai. Set PARALLEL_SKILLS_INDEX_URL to use a custom index.

        Skills install to .agents/skills, and also to .claude/skills when a Claude Code
        configuration directory is present, since Claude Code reads only its own directory.
        """
        pass

    def print_locations(result: dict) -> None:
        locations = result["install_dirs"]
        label = "Locations" if len(locations) > 1 else "Location"
        console.print(f"{label}: [cyan]{', '.join(locations)}[/cyan]")

    def print_skipped(result: dict) -> None:
        skipped = result.get("skipped_skills") or []
        if not skipped:
            return

        console.print(f"[yellow]Skipped ({len(skipped)}):[/yellow]")
        for entry in skipped:
            console.print(
                f"[yellow]  {entry['skill']} — {entry['install_dir']}/{entry['skill']} already exists "
                f"and was not installed by parallel-cli, so it was left untouched[/yellow]"
            )

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
        help="Install into the detected project root — .agents/skills, plus .claude/skills "
        "when Claude Code is present (default is global install).",
    )
    @click.option(
        "--skill",
        "skill_names",
        multiple=True,
        help="Skill name to install (repeatable). Defaults to all. Other managed skills will be removed.",
    )
    @click.option("--json", "output_json", is_flag=True, help="Output as JSON")
    def skills_install(project: bool, skill_names: tuple[str, ...], output_json: bool) -> None:
        """Install or update Parallel skills from skills.parallel.ai.

        Run again after updating parallel-cli to refresh managed skills.

        When --skill is provided, the managed install set is replaced with exactly
        the listed skills. Unmanaged skills are never overwritten or removed.
        """
        from parallel_web_tools.core.skills import (
            SkillsError,
            SkillsInputError,
            SkillsInstallLocationError,
            install_skills,
            resolve_install_dirs,
        )

        try:
            install_dirs = resolve_install_dirs(project=project)
            result = install_skills(
                install_dirs=install_dirs,
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
        print_locations(result)
        console.print(f"Ref: [cyan]{result['ref']}[/cyan]")
        console.print(f"Installed ({result['count']}): [cyan]{', '.join(result['installed_skills'])}[/cyan]")
        console.print(f"Files written: [cyan]{result['file_count']}[/cyan]")
        print_skipped(result)

    @skills.command(name="uninstall")
    @click.option(
        "--project",
        is_flag=True,
        help="Uninstall from the detected project root's skill directories (default is global install).",
    )
    @click.option("--json", "output_json", is_flag=True, help="Output as JSON")
    def skills_uninstall(project: bool, output_json: bool) -> None:
        """Uninstall skills previously installed by parallel-cli."""
        from parallel_web_tools.core.skills import SkillsInstallLocationError, resolve_install_dirs, uninstall_skills

        try:
            install_dirs = resolve_install_dirs(project=project)
            result = uninstall_skills(install_dirs=install_dirs)
        except SkillsInstallLocationError as e:
            handle_error(e, output_json=output_json, exit_code=exit_bad_input, prefix="Skills uninstall failed")
        except Exception as e:
            handle_error(e, output_json=output_json, exit_code=exit_api_error, prefix="Skills uninstall failed")

        if output_json:
            print(json.dumps(result, indent=2))
            return

        if result["count"] == 0:
            console.print("[yellow]No managed skills found to uninstall[/yellow]")
            print_locations(result)
            return

        console.print("[bold green]Skills uninstalled[/bold green]")
        print_locations(result)
        console.print(f"Removed ({result['count']}): [cyan]{', '.join(result['removed_skills'])}[/cyan]")

    @skills.command(name="reinstall")
    @click.option(
        "--project",
        is_flag=True,
        help="Reinstall in the detected project root's skill directories (default is global install).",
    )
    @click.option(
        "--skill",
        "skill_names",
        multiple=True,
        help="Skill name to reinstall (repeatable). Defaults to all. Other managed skills will be removed.",
    )
    @click.option("--json", "output_json", is_flag=True, help="Output as JSON")
    def skills_reinstall(project: bool, skill_names: tuple[str, ...], output_json: bool) -> None:
        """Reinstall Parallel skills (uninstall managed set then install fresh).

        When --skill is provided, the managed install set is replaced with exactly
        the listed skills. Unmanaged skills are never overwritten or removed.
        """
        from parallel_web_tools.core.skills import (
            SkillsError,
            SkillsInputError,
            SkillsInstallLocationError,
            reinstall_skills,
            resolve_install_dirs,
        )

        try:
            install_dirs = resolve_install_dirs(project=project)
            result = reinstall_skills(
                install_dirs=install_dirs,
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
        print_locations(result)
        console.print(f"Ref: [cyan]{result['ref']}[/cyan]")
        console.print(f"Removed ({result['removed_count']}): [cyan]{', '.join(result['removed_skills'])}[/cyan]")
        console.print(f"Installed ({result['installed_count']}): [cyan]{', '.join(result['installed_skills'])}[/cyan]")
        console.print(f"Files written: [cyan]{result['file_count']}[/cyan]")
        print_skipped(result)

    return skills
