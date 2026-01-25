# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec file for parallel-cli.

Build with:
    pyinstaller parallel-web-tools.spec

Or use the build script:
    python scripts/build.py
"""

import sys
from pathlib import Path

# Get certifi certificate bundle path for SSL support
import certifi
certifi_path = Path(certifi.where())

# Get the project root
project_root = Path(SPECPATH)

a = Analysis(
    [str(project_root / 'parallel_web_tools' / 'cli' / 'commands.py')],
    pathex=[str(project_root)],
    binaries=[],
    datas=[(str(certifi_path), 'certifi')],
    hiddenimports=[
        # Core package
        'parallel_web_tools',
        'parallel_web_tools.core',
        'parallel_web_tools.core.auth',
        'parallel_web_tools.core.batch',
        'parallel_web_tools.core.runner',
        'parallel_web_tools.core.schema',
        # CLI
        'parallel_web_tools.cli',
        'parallel_web_tools.cli.commands',
        'parallel_web_tools.cli.planner',
        # Processors (for local file/db enrichment)
        # Note: bigquery processor not included - requires sqlalchemy-bigquery driver
        'parallel_web_tools.processors',
        'parallel_web_tools.processors.csv',
        'parallel_web_tools.processors.duckdb',
        # Note: Deploy commands (bigquery, snowflake) are NOT included in standalone CLI
        # They require: pip install parallel-web-tools[snowflake] or [bigquery]
        # Dependencies that might not be auto-detected
        'click',
        'questionary',
        'rich',
        'yaml',
        'dotenv',
        'pydantic',
        'httpx',
        'parallel',
        'duckdb',
        'sqlalchemy',
        'pandas',
        'certifi',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[str(project_root / 'scripts' / 'runtime_hook_ssl.py')],
    excludes=[
        # Exclude unnecessary modules to reduce size
        'tkinter',
        'matplotlib',
        'PIL',
        'numpy.tests',
        'pandas.tests',
    ],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='parallel-cli',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
