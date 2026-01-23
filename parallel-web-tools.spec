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
        # Processors
        'parallel_web_tools.processors',
        'parallel_web_tools.processors.csv',
        'parallel_web_tools.processors.duckdb',
        'parallel_web_tools.processors.bigquery',
        # Integrations
        'parallel_web_tools.polars',
        'parallel_web_tools.polars.enrich',
        'parallel_web_tools.duckdb',
        'parallel_web_tools.duckdb.batch',
        'parallel_web_tools.duckdb.udf',
        'parallel_web_tools.bigquery',
        'parallel_web_tools.bigquery.deploy',
        'parallel_web_tools.snowflake',
        'parallel_web_tools.snowflake.deploy',
        'parallel_web_tools.spark',
        'parallel_web_tools.spark.udf',
        'parallel_web_tools.spark.streaming',
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
        'polars',
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
