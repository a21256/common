"""
Django integration layer for db_backup.

Reads connection parameters from settings.DATABASES and provides
convenience functions and a management command interface.

Requires: pip install yumoyi-common[django]
"""

from __future__ import annotations

import logging
from io import StringIO
from typing import Optional, Sequence

from django.conf import settings
from django.core.management import call_command

from .db_backup import (
    BackupResult,
    RestoreResult,
    backup_database,
    backup_tables,
    restore_backup,
    cleanup_old_backups,
    DEFAULT_KEEP,
    DEFAULT_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)


def _get_db_params(db_alias: str = "default") -> dict:
    """Extract connection parameters from Django settings."""
    db = settings.DATABASES[db_alias]
    engine = db.get("ENGINE", "")
    if "mysql" not in engine:
        raise ValueError(
            f"Database '{db_alias}' uses engine '{engine}', "
            f"but only MySQL is supported for mysqldump backup."
        )
    return {
        "host": db.get("HOST", "localhost") or "localhost",
        "port": int(db.get("PORT", 3306) or 3306),
        "user": db.get("USER", "root"),
        "password": db.get("PASSWORD", ""),
        "database": db.get("NAME", ""),
        "charset": db.get("OPTIONS", {}).get("charset", "utf8mb4"),
    }


def get_migration_state(db_alias: str = "default") -> str:
    """Capture current migration state as a string for audit purposes."""
    out = StringIO()
    try:
        call_command("showmigrations", "--list", stdout=out, verbosity=0)
    except Exception:
        logger.warning("Failed to capture migration state")
        return ""
    return out.getvalue()


def backup_current_database(
    *,
    output_dir: str,
    tables: Optional[Sequence[str]] = None,
    compress: bool = False,
    db_alias: str = "default",
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> BackupResult:
    """Backup the current Django database (full or specific tables).

    Automatically reads connection parameters from settings.DATABASES.
    Records migration state in the result.
    """
    params = _get_db_params(db_alias)
    migration_state = get_migration_state(db_alias)

    if tables:
        result = backup_tables(
            **params, tables=tables,
            output_dir=output_dir, compress=compress, timeout=timeout,
        )
    else:
        result = backup_database(
            **params,
            output_dir=output_dir, compress=compress, timeout=timeout,
        )

    if result.success and migration_state:
        logger.info("Migration state at backup time:\n%s", migration_state)

    return result


def restore_to_current_database(
    *,
    backup_file: str,
    db_alias: str = "default",
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> RestoreResult:
    """Restore a backup file to the current Django database.

    Automatically reads connection parameters from settings.DATABASES.
    """
    params = _get_db_params(db_alias)
    return restore_backup(
        **params, backup_file=backup_file, timeout=timeout,
    )


def cleanup_current_database_backups(
    *,
    output_dir: str,
    db_alias: str = "default",
    keep: int = DEFAULT_KEEP,
) -> int:
    """Cleanup old backups for the current Django database."""
    params = _get_db_params(db_alias)
    return cleanup_old_backups(
        output_dir=output_dir, prefix=params["database"], keep=keep,
    )
