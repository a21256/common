"""
Database backup/restore utilities using mysqldump/mysql CLI.

Pure Python -- no Django dependency. Connection parameters are passed
via ConnectionConfig so this module can be used in any Python project.
"""

from __future__ import annotations

import gzip
import logging
import os
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence

logger = logging.getLogger(__name__)

# ==================== Constants ====================

DEFAULT_MYSQLDUMP = "mysqldump"
DEFAULT_MYSQL = "mysql"
DEFAULT_PORT = 3306
DEFAULT_CHARSET = "utf8mb4"
DEFAULT_KEEP = 5
DEFAULT_TIMEOUT_SECONDS = 300
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
MAX_TABLES_IN_FILENAME = 3
STREAM_CHUNK_SIZE = 16 * 1024 * 1024  # 16 MB

EXT_SQL = ".sql"
EXT_SQL_GZ = ".sql.gz"
MYSQL_PWD_ENV = "MYSQL_PWD"
DEFAULT_DUMP_FLAGS = ("--single-transaction", "--routines", "--triggers")


# ==================== Config & Result dataclasses ====================

@dataclass(frozen=True)
class ConnectionConfig:
    """MySQL connection parameters bundle.

    Use this to avoid repeating host/port/user/password/database/charset
    in every function call.
    """
    host: str
    user: str
    password: str
    database: str
    port: int = DEFAULT_PORT
    charset: str = DEFAULT_CHARSET


@dataclass
class BackupResult:
    success: bool
    file_path: str = ""
    file_size: int = 0
    duration: float = 0.0
    error: str = ""
    tables: Optional[List[str]] = None  # None = full backup, list = specific tables
    migration_state: str = ""


@dataclass
class RestoreResult:
    success: bool
    duration: float = 0.0
    error: str = ""


@dataclass
class ListTablesResult:
    """Result of list_tables -- distinguishes empty database from error."""
    success: bool
    tables: List[str] = field(default_factory=list)
    error: str = ""


# ==================== Core functions ====================

def backup_database(
    *,
    config: ConnectionConfig,
    output_dir: str,
    compress: bool = False,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysqldump_path: str = DEFAULT_MYSQLDUMP,
    extra_args: Sequence[str] = (),
) -> BackupResult:
    """Full database backup via mysqldump.

    Streams mysqldump output directly to file to avoid holding the
    entire dump in memory.
    """
    return _run_backup(
        config=config, tables=None, output_dir=output_dir,
        compress=compress, timeout=timeout,
        mysqldump_path=mysqldump_path, extra_args=extra_args,
    )


def backup_tables(
    *,
    config: ConnectionConfig,
    tables: Sequence[str],
    output_dir: str,
    compress: bool = False,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysqldump_path: str = DEFAULT_MYSQLDUMP,
    extra_args: Sequence[str] = (),
) -> BackupResult:
    """Backup specific tables via mysqldump."""
    if not tables:
        return BackupResult(success=False, error="No tables specified")

    return _run_backup(
        config=config, tables=list(tables), output_dir=output_dir,
        compress=compress, timeout=timeout,
        mysqldump_path=mysqldump_path, extra_args=extra_args,
    )


def restore_backup(
    *,
    config: ConnectionConfig,
    backup_file: str,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysql_path: str = DEFAULT_MYSQL,
    extra_args: Sequence[str] = (),
) -> RestoreResult:
    """Restore a database from a .sql or .sql.gz backup file.

    Streams file content to mysql stdin to avoid loading the full
    backup into memory.
    """
    return _run_restore(
        config=config, backup_file=backup_file,
        timeout=timeout, mysql_path=mysql_path,
        extra_args=extra_args, log_prefix="restore",
    )


def restore_tables(
    *,
    config: ConnectionConfig,
    backup_file: str,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysql_path: str = DEFAULT_MYSQL,
    extra_args: Sequence[str] = (),
) -> RestoreResult:
    """Restore specific tables from a table-level backup file.

    Functionally identical to restore_backup() -- a mysqldump file
    produced by backup_tables() already contains only the selected
    tables. This function exists to make the intent explicit at call
    sites and in logs (logged as ``restore_tables`` instead of
    ``restore``).
    """
    return _run_restore(
        config=config, backup_file=backup_file,
        timeout=timeout, mysql_path=mysql_path,
        extra_args=extra_args, log_prefix="restore_tables",
    )


def list_tables(
    *,
    config: ConnectionConfig,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysql_path: str = DEFAULT_MYSQL,
) -> ListTablesResult:
    """List all tables in the given database via ``SHOW TABLES``.

    Returns ListTablesResult with ``success=True`` and sorted table list
    on success, or ``success=False`` with error message on failure.
    """
    mysql_bin = shutil.which(mysql_path)
    if not mysql_bin:
        return ListTablesResult(
            success=False, error=f"mysql client not found: {mysql_path}",
        )

    cmd = [
        mysql_bin,
        f"--host={config.host}",
        f"--port={config.port}",
        f"--user={config.user}",
        f"--default-character-set={config.charset}",
        "--batch",
        "--skip-column-names",
        "-e", "SHOW TABLES",
        config.database,
    ]
    env = {**os.environ, MYSQL_PWD_ENV: config.password}

    try:
        proc = subprocess.run(
            cmd, capture_output=True, env=env, timeout=timeout,
        )
        if proc.returncode != 0:
            err = proc.stderr.decode("utf-8", errors="replace").strip()
            logger.error("SHOW TABLES failed: %s", err)
            return ListTablesResult(success=False, error=err)

        output = proc.stdout.decode("utf-8", errors="replace").strip()
        if not output:
            return ListTablesResult(success=True, tables=[])
        tables = sorted(line.strip() for line in output.splitlines() if line.strip())
        return ListTablesResult(success=True, tables=tables)

    except subprocess.TimeoutExpired:
        logger.error("SHOW TABLES timed out after %ds", timeout)
        return ListTablesResult(
            success=False, error=f"SHOW TABLES timed out after {timeout}s",
        )
    except Exception as exc:
        logger.exception("list_tables failed")
        return ListTablesResult(success=False, error=str(exc))


def cleanup_old_backups(
    *,
    output_dir: str,
    prefix: str,
    keep: int = DEFAULT_KEEP,
    pattern: Optional[str] = None,
) -> int:
    """Remove old backup files, keeping the most recent ``keep`` files.

    By default, matches files starting with ``{prefix}_`` and ending
    with ``.sql`` or ``.sql.gz``.  The trailing underscore prevents
    ``prefix="myapp"`` from matching ``myapp2_...`` files.

    If ``pattern`` is provided, it is used as a glob pattern instead
    (e.g. ``"mydb_[0-9]*.sql*"`` to match only full-database backups).

    Returns the number of files actually deleted (not attempted).
    """
    dir_path = Path(output_dir)
    if not dir_path.is_dir():
        return 0

    boundary = f"{prefix}_"

    if pattern:
        candidates_iter = dir_path.glob(pattern)
    else:
        candidates_iter = (
            f for f in dir_path.iterdir()
            if f.name.startswith(boundary)
            and (f.suffix == EXT_SQL or f.name.endswith(EXT_SQL_GZ))
        )

    candidates = sorted(
        candidates_iter,
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )

    to_delete = candidates[keep:]
    deleted = 0
    for f in to_delete:
        try:
            f.unlink()
            logger.info("Cleaned up old backup: %s", f.name)
            deleted += 1
        except OSError:
            logger.warning("Failed to delete old backup: %s", f.name)

    return deleted


# ==================== Internal ====================

def _table_suffix(tables: Optional[List[str]]) -> str:
    """Build filename suffix from table list.

    Tables are sorted for deterministic filenames regardless of input
    order.

    - No tables        -> ""
    - 1-3 tables       -> "_{t1}_{t2}_{t3}"
    - >3 tables        -> "_{N}tables"
    """
    if not tables:
        return ""
    ordered = sorted(tables)
    if len(ordered) <= MAX_TABLES_IN_FILENAME:
        return "_" + "_".join(ordered)
    return f"_{len(ordered)}tables"


def _is_gzipped(path: Path) -> bool:
    """Check if path is a gzip-compressed SQL file (.sql.gz only)."""
    return path.name.endswith(EXT_SQL_GZ)


def _stream_with_timeout(src, dst, timeout: float) -> None:
    """Copy *src* -> *dst* in a daemon thread with a hard timeout.

    Unlike a simple loop-with-check, this enforces the deadline even
    when a single ``read()`` or ``write()`` call blocks (e.g. because
    the subprocess is hung).  The caller must kill the subprocess after
    a ``TimeoutExpired`` to unblock the stuck thread.
    """
    exc_box: list = []

    def _worker() -> None:
        try:
            while True:
                chunk = src.read(STREAM_CHUNK_SIZE)
                if not chunk:
                    break
                dst.write(chunk)
        except Exception as exc:
            exc_box.append(exc)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join(max(0, timeout))

    if t.is_alive():
        raise subprocess.TimeoutExpired("stream", timeout)
    if exc_box:
        raise exc_box[0]


def _drain_pipe_async(pipe):
    """Start draining *pipe* in a daemon thread.

    Prevents pipe-buffer deadlock: if the child process writes enough
    stderr to fill the OS pipe buffer (~64 KB), it blocks until someone
    reads from the pipe.  Running the read in a background thread keeps
    the buffer drained while the main data flow continues.

    Returns ``(thread, result_list)``.  After the thread joins,
    ``result_list[0]`` contains the bytes read (or ``b""`` on error).
    """
    result: list = []

    def _worker() -> None:
        try:
            result.append(pipe.read())
        except Exception:
            result.append(b"")

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    return t, result


def _safe_unlink(path: Path) -> None:
    """Remove file if it exists, ignoring errors."""
    try:
        if path.exists():
            path.unlink()
    except OSError:
        pass


def _run_backup(
    *,
    config: ConnectionConfig,
    tables: Optional[List[str]],
    output_dir: str,
    compress: bool,
    timeout: int,
    mysqldump_path: str,
    extra_args: Sequence[str],
) -> BackupResult:
    """Internal: execute mysqldump and stream output to file."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    mysqldump_bin = shutil.which(mysqldump_path)
    if not mysqldump_bin:
        return BackupResult(success=False, error=f"mysqldump not found: {mysqldump_path}")

    timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)
    suffix = _table_suffix(tables)
    ext = EXT_SQL_GZ if compress else EXT_SQL
    filename = f"{config.database}{suffix}_{timestamp}{ext}"
    file_path = out_dir / filename

    cmd = [
        mysqldump_bin,
        f"--host={config.host}",
        f"--port={config.port}",
        f"--user={config.user}",
        f"--default-character-set={config.charset}",
        *DEFAULT_DUMP_FLAGS,
        *extra_args,
        config.database,
    ]
    if tables:
        cmd.extend(tables)

    env = {**os.environ, MYSQL_PWD_ENV: config.password}

    start = time.monotonic()
    try:
        if compress:
            # Stream: mysqldump stdout -> gzip file (no full-buffer in memory)
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
            )
            stderr_t, stderr_buf = _drain_pipe_async(proc.stderr)
            try:
                with gzip.open(file_path, "wb") as gz:
                    remaining = timeout - (time.monotonic() - start)
                    _stream_with_timeout(proc.stdout, gz, remaining)
                remaining = max(1, int(timeout - (time.monotonic() - start)))
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                raise
            finally:
                stderr_t.join(5)
            stderr_data = stderr_buf[0] if stderr_buf else b""
        else:
            # Stream: mysqldump stdout -> plain file directly
            with open(file_path, "wb") as f:
                proc = subprocess.run(
                    cmd, stdout=f, stderr=subprocess.PIPE, env=env, timeout=timeout,
                )
            stderr_data = proc.stderr

        duration = time.monotonic() - start

        if proc.returncode != 0:
            err = stderr_data.decode("utf-8", errors="replace").strip()
            logger.error("mysqldump failed: %s", err)
            _safe_unlink(file_path)
            return BackupResult(success=False, duration=duration, error=err,
                                tables=tables)

        file_size = file_path.stat().st_size
        logger.info("Backup %s -> %s (%d bytes, %.1fs)",
                     config.database, filename, file_size, duration)

        return BackupResult(
            success=True, file_path=str(file_path), file_size=file_size,
            duration=duration, tables=tables,
        )

    except subprocess.TimeoutExpired:
        duration = time.monotonic() - start
        _safe_unlink(file_path)
        return BackupResult(success=False, duration=duration,
                            error=f"mysqldump timed out after {timeout}s",
                            tables=tables)
    except Exception as exc:
        duration = time.monotonic() - start
        _safe_unlink(file_path)
        logger.exception("Backup failed")
        return BackupResult(success=False, duration=duration, error=str(exc),
                            tables=tables)


def _run_restore(
    *,
    config: ConnectionConfig,
    backup_file: str,
    timeout: int,
    mysql_path: str,
    extra_args: Sequence[str],
    log_prefix: str,
) -> RestoreResult:
    """Internal: pipe backup file into mysql client."""
    backup_path = Path(backup_file)
    if not backup_path.exists():
        return RestoreResult(success=False, error=f"Backup file not found: {backup_file}")

    mysql_bin = shutil.which(mysql_path)
    if not mysql_bin:
        return RestoreResult(success=False, error=f"mysql client not found: {mysql_path}")

    cmd = [
        mysql_bin,
        f"--host={config.host}",
        f"--port={config.port}",
        f"--user={config.user}",
        f"--default-character-set={config.charset}",
        *extra_args,
        config.database,
    ]
    env = {**os.environ, MYSQL_PWD_ENV: config.password}

    is_gz = _is_gzipped(backup_path)

    start = time.monotonic()
    try:
        if is_gz:
            # Stream: gzip file -> mysql stdin (no full-decompress in memory)
            proc = subprocess.Popen(
                cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
            )
            stderr_t, stderr_buf = _drain_pipe_async(proc.stderr)
            try:
                with gzip.open(backup_path, "rb") as gz:
                    remaining = timeout - (time.monotonic() - start)
                    _stream_with_timeout(gz, proc.stdin, remaining)
                proc.stdin.close()
                remaining = max(1, int(timeout - (time.monotonic() - start)))
                proc.wait(timeout=remaining)
            except BrokenPipeError:
                proc.wait()
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                raise
            finally:
                stderr_t.join(5)
            stderr_data = stderr_buf[0] if stderr_buf else b""
        else:
            # Stream: file -> mysql stdin directly
            with open(backup_path, "rb") as f:
                proc = subprocess.run(
                    cmd, stdin=f, stderr=subprocess.PIPE, env=env, timeout=timeout,
                )
            stderr_data = proc.stderr

        duration = time.monotonic() - start

        if proc.returncode != 0:
            err = stderr_data.decode("utf-8", errors="replace").strip()
            logger.error("%s failed: %s", log_prefix, err)
            return RestoreResult(success=False, duration=duration, error=err)

        logger.info("%s: %s -> %s in %.1fs",
                     log_prefix, backup_file, config.database, duration)
        return RestoreResult(success=True, duration=duration)

    except subprocess.TimeoutExpired:
        duration = time.monotonic() - start
        return RestoreResult(
            success=False, duration=duration,
            error=f"{log_prefix} timed out after {timeout}s",
        )
    except Exception as exc:
        duration = time.monotonic() - start
        logger.exception("%s failed", log_prefix)
        return RestoreResult(success=False, duration=duration, error=str(exc))
