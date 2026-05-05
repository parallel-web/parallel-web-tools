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
        """Install and manage Parallel agent skills.

        Set GH_TOKEN for higher GitHub API rate limits when fetching skills.
        """
        pass

    @skills.command(name="list")
    @click.option("--json", "output_json", is_flag=True, help="Output as JSON")
    def skills_list(output_json: bool) -> None:
        """List available Parallel skills from GitHub."""
        from parallel_web_tools.core.skills import SkillsError, get_skills_repo_ref, list_remote_skills

        try:
            ref = get_skills_repo_ref()
            skill_names = list_remote_skills(ref=ref)
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
        """Install Parallel skills from GitHub.

        When --skill is provided, the managed install set is replaced with exactly
        the listed skills.
        """
        from parallel_web_tools.core.skills import (
            SkillsError,
            SkillsInputError,
            SkillsInstallLocationError,
            get_skills_repo_ref,
            install_skills,
            resolve_install_dir,
        )

        try:
            install_dir = resolve_install_dir(project=project)
            result = install_skills(
                install_dir=install_dir,
                selected_skills=list(skill_names) or None,
                ref=get_skills_repo_ref(),
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
            get_skills_repo_ref,
            reinstall_skills,
            resolve_install_dir,
        )

        try:
            install_dir = resolve_install_dir(project=project)
            result = reinstall_skills(
                install_dir=install_dir,
                selected_skills=list(skill_names) or None,
                ref=get_skills_repo_ref(),
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

    return skills
