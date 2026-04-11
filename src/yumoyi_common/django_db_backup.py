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
    ConnectionConfig,
    BackupResult,
    RestoreResult,
    ListTablesResult,
    backup_database,
    backup_tables,
    restore_backup,
    restore_tables,
    list_tables,
    list_backups,
    cleanup_old_backups,
    DEFAULT_KEEP,
    DEFAULT_MYSQL,
    DEFAULT_MYSQLDUMP,
    DEFAULT_PORT,
    DEFAULT_CHARSET,
    DEFAULT_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)


def _get_connection_config(db_alias: str = "default") -> ConnectionConfig:
    """Extract connection parameters from Django settings."""
    try:
        db = settings.DATABASES[db_alias]
    except KeyError:
        available = ", ".join(sorted(settings.DATABASES)) or "(none)"
        raise ValueError(
            f"Database alias '{db_alias}' not found in settings.DATABASES. "
            f"Available: {available}"
        )
    engine = db.get("ENGINE", "")
    if "mysql" not in engine:
        raise ValueError(
            f"Database '{db_alias}' uses engine '{engine}', "
            f"but only MySQL is supported for mysqldump backup."
        )
    return ConnectionConfig(
        host=db.get("HOST", "localhost") or "localhost",
        port=int(db.get("PORT", DEFAULT_PORT) or DEFAULT_PORT),
        user=db.get("USER", "root"),
        password=db.get("PASSWORD", ""),
        database=db.get("NAME", ""),
        charset=db.get("OPTIONS", {}).get("charset", DEFAULT_CHARSET),
    )


def get_migration_state(db_alias: str = "default") -> str:
    """Capture current migration state as a string for audit purposes."""
    out = StringIO()
    try:
        call_command(
            "showmigrations", "--list",
            database=db_alias,
            stdout=out, verbosity=0,
        )
    except Exception:
        logger.warning("Failed to capture migration state for '%s'", db_alias)
        return ""
    return out.getvalue()


def backup_current_database(
    *,
    output_dir: str,
    tables: Optional[Sequence[str]] = None,
    compress: bool = False,
    db_alias: str = "default",
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysqldump_path: str = DEFAULT_MYSQLDUMP,
    mysql_path: str = DEFAULT_MYSQL,
    extra_args: Sequence[str] = (),
    collect_metadata: bool = True,
    tag: str = "",
) -> BackupResult:
    """Backup the current Django database (full or specific tables).

    Automatically reads connection parameters from settings.DATABASES.
    Captures migration state and stores it in ``BackupResult.migration_state``.
    """
    config = _get_connection_config(db_alias)
    migration_state = get_migration_state(db_alias)

    if tables:
        result = backup_tables(
            config=config, tables=tables,
            output_dir=output_dir, compress=compress,
            timeout=timeout, mysqldump_path=mysqldump_path,
            mysql_path=mysql_path,
            extra_args=extra_args, collect_metadata=collect_metadata,
            tag=tag,
        )
    else:
        result = backup_database(
            config=config,
            output_dir=output_dir, compress=compress,
            timeout=timeout, mysqldump_path=mysqldump_path,
            mysql_path=mysql_path,
            extra_args=extra_args, collect_metadata=collect_metadata,
            tag=tag,
        )

    if result.success and migration_state:
        result.migration_state = migration_state
        logger.info("Migration state at backup time:\n%s", migration_state)

    return result


def restore_to_current_database(
    *,
    backup_file: str,
    db_alias: str = "default",
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysql_path: str = DEFAULT_MYSQL,
    extra_args: Sequence[str] = (),
) -> RestoreResult:
    """Restore a backup file to the current Django database.

    Automatically reads connection parameters from settings.DATABASES.
    """
    config = _get_connection_config(db_alias)
    return restore_backup(
        config=config, backup_file=backup_file,
        timeout=timeout, mysql_path=mysql_path,
        extra_args=extra_args,
    )


def cleanup_current_database_backups(
    *,
    output_dir: str,
    db_alias: str = "default",
    keep: int = DEFAULT_KEEP,
    pattern: Optional[str] = None,
) -> int:
    """Cleanup old backups for the current Django database."""
    config = _get_connection_config(db_alias)
    return cleanup_old_backups(
        output_dir=output_dir, prefix=config.database,
        keep=keep, pattern=pattern,
    )


def restore_tables_to_current_database(
    *,
    backup_file: str,
    db_alias: str = "default",
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysql_path: str = DEFAULT_MYSQL,
    extra_args: Sequence[str] = (),
) -> RestoreResult:
    """Restore a table-level backup to the current Django database.

    Automatically reads connection parameters from settings.DATABASES.
    """
    config = _get_connection_config(db_alias)
    return restore_tables(
        config=config, backup_file=backup_file,
        timeout=timeout, mysql_path=mysql_path,
        extra_args=extra_args,
    )


def list_current_database_backups(
    *,
    output_dir: str,
    db_alias: str = "default",
) -> list:
    """List backup files for the current Django database, newest first.

    Uses the database name as prefix to filter backups.
    Each item includes metadata loaded from sidecar .meta.json if available.
    """
    config = _get_connection_config(db_alias)
    return list_backups(output_dir=output_dir, prefix=config.database)


def list_current_database_tables(
    db_alias: str = "default",
    mysql_path: str = DEFAULT_MYSQL,
) -> ListTablesResult:
    """List all tables in the current Django database.

    Automatically reads connection parameters from settings.DATABASES.
    Returns ListTablesResult with success flag and table list.
    """
    config = _get_connection_config(db_alias)
    return list_tables(config=config, mysql_path=mysql_path)
