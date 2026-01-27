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
        'parallel_web_tools.core.research',
        'parallel_web_tools.core.result',
        # CLI (standalone mode - no planner)
        'parallel_web_tools.cli',
        'parallel_web_tools.cli.commands',
        # Processors (CSV only in standalone - DuckDB/BigQuery require pip install)
        'parallel_web_tools.processors',
        'parallel_web_tools.processors.csv',
        # Note: These features are NOT included in standalone CLI:
        # - enrich plan (interactive wizard) - requires questionary, duckdb
        # - enrich run with YAML config - requires pyyaml
        # - DuckDB processor - requires duckdb, polars
        # - BigQuery processor - requires sqlalchemy-bigquery
        # - Deploy commands - requires snowflake-connector or gcloud
        # For these features: pip install parallel-web-tools[all]
        # Dependencies that might not be auto-detected
        'click',
        'rich',
        'dotenv',
        'pydantic',
        'httpx',
        'parallel',
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
        'numpy',
        'pandas',
        'polars',
        'pyarrow',
        'duckdb',
        'sqlalchemy',
        'questionary',
        'prompt_toolkit',
        'yaml',
        'pyyaml',
    ],
    noarchive=False,
)

pyz = PYZ(a.pure)

# Use onedir mode for faster startup (no extraction needed)
# Distribution is a folder instead of single file, but startup is ~0.2s vs ~1s
exe = EXE(
    pyz,
    a.scripts,
    [],  # Don't include binaries/datas in EXE (they go in COLLECT)
    exclude_binaries=True,
    name='parallel-cli',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='parallel-cli',
)
