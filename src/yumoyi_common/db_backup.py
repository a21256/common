"""
Database backup/restore utilities using mysqldump/mysql CLI.

Pure Python — no Django dependency. Connection parameters are passed
explicitly so this module can be used in any Python project.
"""

from __future__ import annotations

import gzip
import logging
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence

logger = logging.getLogger(__name__)

# ==================== Constants ====================

DEFAULT_MYSQLDUMP = "mysqldump"
DEFAULT_MYSQL = "mysql"
DEFAULT_KEEP = 5
DEFAULT_TIMEOUT_SECONDS = 300
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"


# ==================== Result dataclass ====================

@dataclass
class BackupResult:
    success: bool
    file_path: str = ""
    file_size: int = 0
    duration: float = 0.0
    error: str = ""
    tables: List[str] = field(default_factory=list)


@dataclass
class RestoreResult:
    success: bool
    duration: float = 0.0
    error: str = ""


# ==================== Core functions ====================

def backup_database(
    *,
    host: str,
    port: int = 3306,
    user: str,
    password: str,
    database: str,
    output_dir: str,
    compress: bool = False,
    charset: str = "utf8mb4",
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysqldump_path: str = DEFAULT_MYSQLDUMP,
) -> BackupResult:
    """Full database backup via mysqldump.

    Returns a BackupResult with file_path, file_size, and duration.
    """
    return _run_backup(
        host=host, port=port, user=user, password=password,
        database=database, tables=None, output_dir=output_dir,
        compress=compress, charset=charset, timeout=timeout,
        mysqldump_path=mysqldump_path,
    )


def backup_tables(
    *,
    host: str,
    port: int = 3306,
    user: str,
    password: str,
    database: str,
    tables: Sequence[str],
    output_dir: str,
    compress: bool = False,
    charset: str = "utf8mb4",
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysqldump_path: str = DEFAULT_MYSQLDUMP,
) -> BackupResult:
    """Backup specific tables via mysqldump.

    Returns a BackupResult with file_path, file_size, and duration.
    """
    if not tables:
        return BackupResult(success=False, error="No tables specified")

    return _run_backup(
        host=host, port=port, user=user, password=password,
        database=database, tables=list(tables), output_dir=output_dir,
        compress=compress, charset=charset, timeout=timeout,
        mysqldump_path=mysqldump_path,
    )


def restore_backup(
    *,
    host: str,
    port: int = 3306,
    user: str,
    password: str,
    database: str,
    backup_file: str,
    charset: str = "utf8mb4",
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysql_path: str = DEFAULT_MYSQL,
) -> RestoreResult:
    """Restore a database from a .sql or .sql.gz backup file.

    Automatically detects gzip compression from file extension.
    """
    backup_path = Path(backup_file)
    if not backup_path.exists():
        return RestoreResult(success=False, error=f"Backup file not found: {backup_file}")

    mysql_bin = shutil.which(mysql_path)
    if not mysql_bin:
        return RestoreResult(success=False, error=f"mysql client not found: {mysql_path}")

    cmd = [
        mysql_bin,
        f"--host={host}",
        f"--port={port}",
        f"--user={user}",
        f"--default-character-set={charset}",
        database,
    ]
    env = {**os.environ, "MYSQL_PWD": password}

    is_gzipped = backup_path.suffix == ".gz" or backup_path.name.endswith(".sql.gz")

    start = time.monotonic()
    try:
        if is_gzipped:
            with gzip.open(backup_path, "rb") as gz:
                proc = subprocess.run(
                    cmd, input=gz.read(), env=env,
                    capture_output=True, timeout=timeout,
                )
        else:
            with open(backup_path, "rb") as f:
                proc = subprocess.run(
                    cmd, input=f.read(), env=env,
                    capture_output=True, timeout=timeout,
                )

        duration = time.monotonic() - start

        if proc.returncode != 0:
            err = proc.stderr.decode("utf-8", errors="replace").strip()
            logger.error("mysql restore failed: %s", err)
            return RestoreResult(success=False, duration=duration, error=err)

        logger.info("Restored %s to %s in %.1fs", backup_file, database, duration)
        return RestoreResult(success=True, duration=duration)

    except subprocess.TimeoutExpired:
        duration = time.monotonic() - start
        return RestoreResult(success=False, duration=duration, error=f"Restore timed out after {timeout}s")
    except Exception as exc:
        duration = time.monotonic() - start
        logger.exception("Restore failed")
        return RestoreResult(success=False, duration=duration, error=str(exc))


def cleanup_old_backups(
    *,
    output_dir: str,
    prefix: str,
    keep: int = DEFAULT_KEEP,
) -> int:
    """Remove old backup files, keeping the most recent `keep` files.

    Matches files starting with `prefix` and ending with .sql or .sql.gz.
    Returns the number of files deleted.
    """
    dir_path = Path(output_dir)
    if not dir_path.is_dir():
        return 0

    candidates = sorted(
        [f for f in dir_path.iterdir()
         if f.name.startswith(prefix) and (f.suffix == ".sql" or f.name.endswith(".sql.gz"))],
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )

    to_delete = candidates[keep:]
    for f in to_delete:
        try:
            f.unlink()
            logger.info("Cleaned up old backup: %s", f.name)
        except OSError:
            logger.warning("Failed to delete old backup: %s", f.name)

    return len(to_delete)


# ==================== Internal ====================

def _run_backup(
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    tables: Optional[List[str]],
    output_dir: str,
    compress: bool,
    charset: str,
    timeout: int,
    mysqldump_path: str,
) -> BackupResult:
    """Internal: execute mysqldump and write output to file."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    mysqldump_bin = shutil.which(mysqldump_path)
    if not mysqldump_bin:
        return BackupResult(success=False, error=f"mysqldump not found: {mysqldump_path}")

    timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)
    if tables:
        suffix = "_tables"
    else:
        suffix = ""
    ext = ".sql.gz" if compress else ".sql"
    filename = f"{database}{suffix}_{timestamp}{ext}"
    file_path = out_dir / filename

    cmd = [
        mysqldump_bin,
        f"--host={host}",
        f"--port={port}",
        f"--user={user}",
        f"--default-character-set={charset}",
        "--single-transaction",
        "--routines",
        "--triggers",
        database,
    ]
    if tables:
        cmd.extend(tables)

    env = {**os.environ, "MYSQL_PWD": password}

    start = time.monotonic()
    try:
        proc = subprocess.run(
            cmd, capture_output=True, env=env, timeout=timeout,
        )

        duration = time.monotonic() - start

        if proc.returncode != 0:
            err = proc.stderr.decode("utf-8", errors="replace").strip()
            logger.error("mysqldump failed: %s", err)
            return BackupResult(success=False, duration=duration, error=err,
                                tables=tables or [])

        data = proc.stdout
        if compress:
            with gzip.open(file_path, "wb") as gz:
                gz.write(data)
        else:
            with open(file_path, "wb") as f:
                f.write(data)

        file_size = file_path.stat().st_size
        logger.info("Backup %s -> %s (%d bytes, %.1fs)", database, filename, file_size, duration)

        return BackupResult(
            success=True, file_path=str(file_path), file_size=file_size,
            duration=duration, tables=tables or [],
        )

    except subprocess.TimeoutExpired:
        duration = time.monotonic() - start
        return BackupResult(success=False, duration=duration,
                            error=f"mysqldump timed out after {timeout}s",
                            tables=tables or [])
    except Exception as exc:
        duration = time.monotonic() - start
        logger.exception("Backup failed")
        return BackupResult(success=False, duration=duration, error=str(exc),
                            tables=tables or [])
