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
import warnings
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
STDERR_DRAIN_TIMEOUT = 5   # seconds to wait for stderr thread after main I/O completes
DEFAULT_COUNT_TIMEOUT = 5  # per-table SELECT COUNT(*) timeout; falls back to estimate
MIN_WAIT_TIMEOUT = 1       # floor for remaining-time calculations (seconds)

# Column indices for information_schema.TABLES query result
# (TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH)
_INFO_COL_NAME = 0
_INFO_COL_ROWS = 1
_INFO_COL_DATA = 2
_INFO_COL_INDEX = 3
_INFO_COL_COUNT = 4        # minimum number of columns expected

# Column indices for SHOW CREATE TABLE result (TABLE_NAME, DDL)
_DDL_COL_VALUE = 1
_DDL_COL_COUNT = 2


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


@dataclass(frozen=True)
class TableStats:
    """Per-table statistics collected at backup time.

    ``row_count`` is exact (via ``SELECT COUNT(*)``) unless the query
    timed out, in which case it falls back to the approximate value
    from ``information_schema.TABLES.TABLE_ROWS`` and ``estimated``
    is set to ``True``.
    """
    name: str
    row_count: int
    estimated: bool = False      # True if row_count is approximate (COUNT timed out)
    data_size: int = 0           # table data size in bytes (DATA_LENGTH)
    index_size: int = 0          # index size in bytes (INDEX_LENGTH)
    ddl: str = ""                # CREATE TABLE statement; populated for table-level backups


@dataclass
class BackupMetadata:
    """Aggregate statistics about what was backed up."""
    table_count: int = 0
    table_stats: List[TableStats] = field(default_factory=list)
    total_data_size: int = 0     # sum of all tables' data_size
    total_index_size: int = 0    # sum of all tables' index_size
    backup_tag: str = ""         # caller-supplied label, e.g. "manual" / "pre_import_auto"


@dataclass
class BackupResult:
    success: bool
    file_path: str = ""
    file_size: int = 0
    duration: float = 0.0
    error: str = ""
    tables: Optional[List[str]] = None  # None = full backup, list = specific tables
    migration_state: str = ""
    metadata: Optional[BackupMetadata] = None


@dataclass
class RestoreResult:
    success: bool
    file_path: str = ""
    duration: float = 0.0
    error: str = ""


@dataclass
class ListTablesResult:
    """Result of list_tables -- distinguishes empty database from error."""
    success: bool
    tables: List[str] = field(default_factory=list)
    error: str = ""


# ==================== Backward compatibility ====================

_FLAT_PARAM_KEYS = frozenset({"host", "port", "user", "password", "database", "charset"})
_COMPAT_WARN_STACKLEVEL = 3  # public func -> _compat_config -> warnings.warn


def _compat_config(kwargs: dict) -> ConnectionConfig:
    """Build ConnectionConfig from legacy flat keyword arguments.

    If the caller passes host/port/user/password/database/charset instead
    of config=ConnectionConfig(...), this helper constructs the config and
    emits a DeprecationWarning.  The flat-param style will be removed in
    a future version.
    """
    present = _FLAT_PARAM_KEYS & kwargs.keys()
    if not present:
        raise TypeError(
            "Missing required keyword argument 'config'. "
            "Pass config=ConnectionConfig(...) or the legacy flat parameters "
            "(host=, user=, password=, database=)."
        )
    warnings.warn(
        "Passing host/port/user/password/database/charset as flat keyword "
        "arguments is deprecated. Use config=ConnectionConfig(...) instead.",
        DeprecationWarning,
        stacklevel=_COMPAT_WARN_STACKLEVEL,
    )
    config = ConnectionConfig(
        host=kwargs.pop("host"),
        user=kwargs.pop("user"),
        password=kwargs.pop("password"),
        database=kwargs.pop("database"),
        port=kwargs.pop("port", DEFAULT_PORT),
        charset=kwargs.pop("charset", DEFAULT_CHARSET),
    )
    if kwargs:
        unexpected = ", ".join(sorted(kwargs))
        raise TypeError(f"Unexpected keyword arguments: {unexpected}")
    return config


# ==================== Core functions ====================

def backup_database(
    *,
    config: Optional[ConnectionConfig] = None,
    output_dir: str,
    compress: bool = False,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysqldump_path: str = DEFAULT_MYSQLDUMP,
    mysql_path: str = DEFAULT_MYSQL,
    extra_args: Sequence[str] = (),
    collect_metadata: bool = True,
    count_timeout: int = DEFAULT_COUNT_TIMEOUT,
    tag: str = "",
    **kwargs,
) -> BackupResult:
    """Full database backup via mysqldump.

    Streams mysqldump output directly to file to avoid holding the
    entire dump in memory.

    Set ``collect_metadata=False`` to skip post-backup table statistics
    (saves one round-trip to the database).  ``count_timeout`` controls
    the per-table ``SELECT COUNT(*)`` timeout in seconds (default 5);
    increase for large tables if you need exact counts.

    ``tag`` is stored in ``BackupResult.metadata.backup_tag`` for audit
    (e.g. ``"manual"``, ``"pre_import_auto"``).

    .. deprecated:: 0.3.0
       Passing ``host``, ``user``, ``password``, ``database`` as flat
       keyword arguments is deprecated.  Use ``config=ConnectionConfig(...)``
       instead.
    """
    if config is None:
        config = _compat_config(kwargs)
    elif kwargs:
        raise TypeError(f"Unexpected keyword arguments: {', '.join(sorted(kwargs))}")
    return _run_backup(
        config=config, tables=None, output_dir=output_dir,
        compress=compress, timeout=timeout,
        mysqldump_path=mysqldump_path, mysql_path=mysql_path,
        extra_args=extra_args, collect_metadata=collect_metadata,
        count_timeout=count_timeout, tag=tag,
    )


def backup_tables(
    *,
    config: Optional[ConnectionConfig] = None,
    tables: Sequence[str],
    output_dir: str,
    compress: bool = False,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysqldump_path: str = DEFAULT_MYSQLDUMP,
    mysql_path: str = DEFAULT_MYSQL,
    extra_args: Sequence[str] = (),
    collect_metadata: bool = True,
    count_timeout: int = DEFAULT_COUNT_TIMEOUT,
    tag: str = "",
    **kwargs,
) -> BackupResult:
    """Backup specific tables via mysqldump.

    Tables are sorted for deterministic filenames; ``result.tables``
    contains the sorted order, not the input order.

    Set ``collect_metadata=False`` to skip post-backup table statistics.

    .. deprecated:: 0.3.0
       Flat connection keyword arguments are deprecated.
       Use ``config=ConnectionConfig(...)`` instead.
    """
    if config is None:
        config = _compat_config(kwargs)
    elif kwargs:
        raise TypeError(f"Unexpected keyword arguments: {', '.join(sorted(kwargs))}")
    if not tables:
        return BackupResult(success=False, error="No tables specified")

    return _run_backup(
        config=config, tables=sorted(tables), output_dir=output_dir,
        compress=compress, timeout=timeout,
        mysqldump_path=mysqldump_path, mysql_path=mysql_path,
        extra_args=extra_args, collect_metadata=collect_metadata,
        count_timeout=count_timeout, tag=tag,
    )


def restore_backup(
    *,
    config: Optional[ConnectionConfig] = None,
    backup_file: str,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysql_path: str = DEFAULT_MYSQL,
    extra_args: Sequence[str] = (),
    **kwargs,
) -> RestoreResult:
    """Restore a database from a .sql or .sql.gz backup file.

    Streams file content to mysql stdin to avoid loading the full
    backup into memory.

    .. deprecated:: 0.3.0
       Flat connection keyword arguments are deprecated.
       Use ``config=ConnectionConfig(...)`` instead.
    """
    if config is None:
        config = _compat_config(kwargs)
    elif kwargs:
        raise TypeError(f"Unexpected keyword arguments: {', '.join(sorted(kwargs))}")
    return _run_restore(
        config=config, backup_file=backup_file,
        timeout=timeout, mysql_path=mysql_path,
        extra_args=extra_args, log_prefix="restore",
    )


def restore_tables(
    *,
    config: Optional[ConnectionConfig] = None,
    backup_file: str,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysql_path: str = DEFAULT_MYSQL,
    extra_args: Sequence[str] = (),
    **kwargs,
) -> RestoreResult:
    """Restore specific tables from a table-level backup file.

    Functionally identical to restore_backup() -- a mysqldump file
    produced by backup_tables() already contains only the selected
    tables. This function exists to make the intent explicit at call
    sites and in logs (logged as ``restore_tables`` instead of
    ``restore``).

    .. deprecated:: 0.3.0
       Flat connection keyword arguments are deprecated.
       Use ``config=ConnectionConfig(...)`` instead.
    """
    if config is None:
        config = _compat_config(kwargs)
    elif kwargs:
        raise TypeError(f"Unexpected keyword arguments: {', '.join(sorted(kwargs))}")
    return _run_restore(
        config=config, backup_file=backup_file,
        timeout=timeout, mysql_path=mysql_path,
        extra_args=extra_args, log_prefix="restore_tables",
    )


def list_tables(
    *,
    config: Optional[ConnectionConfig] = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    mysql_path: str = DEFAULT_MYSQL,
    **kwargs,
) -> ListTablesResult:
    """List all tables in the given database via ``SHOW TABLES``.

    Returns ListTablesResult with ``success=True`` and sorted table list
    on success, or ``success=False`` with error message on failure.

    .. deprecated:: 0.3.0
       Flat connection keyword arguments are deprecated.
       Use ``config=ConnectionConfig(...)`` instead.
    """
    if config is None:
        config = _compat_config(kwargs)
    elif kwargs:
        raise TypeError(f"Unexpected keyword arguments: {', '.join(sorted(kwargs))}")
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
            # Also remove sidecar metadata file
            meta = Path(str(f) + METADATA_EXT)
            if meta.exists():
                meta.unlink(missing_ok=True)
            logger.info("Cleaned up old backup: %s", f.name)
            deleted += 1
        except OSError:
            logger.warning("Failed to delete old backup: %s", f.name)

    return deleted


# ==================== Metadata Persistence ====================

METADATA_EXT = ".meta.json"
METADATA_JSON_INDENT = 2


def save_backup_metadata(backup_file_path: str, metadata: BackupMetadata) -> None:
    """Save BackupMetadata as a JSON sidecar file next to the backup.

    Creates ``{backup_file_path}.meta.json``.  Silently returns on failure.
    """
    import json

    meta_dict = {
        "table_count": metadata.table_count,
        "total_data_size": metadata.total_data_size,
        "total_index_size": metadata.total_index_size,
        "backup_tag": metadata.backup_tag,
        "tables": [
            {
                "name": t.name,
                "row_count": t.row_count,
                "estimated": t.estimated,
                "data_size": t.data_size,
                "index_size": t.index_size,
            }
            for t in metadata.table_stats
        ],
    }
    meta_path = Path(str(backup_file_path) + METADATA_EXT)
    try:
        meta_path.write_text(
            json.dumps(meta_dict, ensure_ascii=False, indent=METADATA_JSON_INDENT),
            encoding="utf-8",
        )
    except Exception:
        logger.warning("Failed to save backup metadata: %s", meta_path)


def load_backup_metadata(backup_file_path: str) -> Optional[BackupMetadata]:
    """Load BackupMetadata from a JSON sidecar file.

    Returns ``None`` if the sidecar does not exist or cannot be parsed.
    """
    import json

    meta_path = Path(str(backup_file_path) + METADATA_EXT)
    if not meta_path.exists():
        return None
    try:
        data = json.loads(meta_path.read_text(encoding="utf-8"))
        table_stats = [
            TableStats(
                name=t["name"],
                row_count=t.get("row_count", 0),
                estimated=t.get("estimated", False),
                data_size=t.get("data_size", 0),
                index_size=t.get("index_size", 0),
            )
            for t in data.get("tables", [])
        ]
        return BackupMetadata(
            table_count=data.get("table_count", 0),
            table_stats=table_stats,
            total_data_size=data.get("total_data_size", 0),
            total_index_size=data.get("total_index_size", 0),
            backup_tag=data.get("backup_tag", ""),
        )
    except Exception:
        logger.warning("Failed to load backup metadata: %s", meta_path)
        return None


def list_backups(
    *,
    output_dir: str,
    prefix: Optional[str] = None,
) -> List[dict]:
    """List backup files in *output_dir*, newest first.

    Each item is a dict with keys: ``name``, ``file_path``, ``size``,
    ``mtime`` (ISO formatted string), ``metadata`` (BackupMetadata or None).

    If *prefix* is given, only files whose name starts with
    ``{prefix}_`` are returned.
    """
    dir_path = Path(output_dir)
    if not dir_path.is_dir():
        return []

    results = []
    for p in sorted(dir_path.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
        if not p.is_file():
            continue
        if p.suffix not in (EXT_SQL,) and not p.name.endswith(EXT_SQL_GZ):
            continue
        if prefix and not p.name.startswith(f"{prefix}_"):
            continue

        stat = p.stat()
        results.append({
            "name": p.name,
            "file_path": str(p),
            "size": stat.st_size,
            "mtime": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            "metadata": load_backup_metadata(str(p)),
        })

    return results


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


def _escape_sql_identifier(name: str) -> str:
    """Escape a value for use in a SQL string literal (single-quoted).

    Escapes single quotes by doubling them: O'Brien -> 'O''Brien'.
    Escapes backslashes: a\\b -> 'a\\\\b'.
    """
    escaped = name.replace("\\", "\\\\").replace("'", "''")
    return f"'{escaped}'"


def _escape_backtick_identifier(name: str) -> str:
    """Escape a value for use as a backtick-quoted SQL identifier.

    Escapes embedded backticks by doubling them: my`table -> `my``table`.
    """
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def _safe_unlink(path: Path) -> None:
    """Remove file if it exists, ignoring errors."""
    try:
        if path.exists():
            path.unlink()
    except OSError:
        pass


def _collect_metadata(
    config: ConnectionConfig,
    tables: Optional[List[str]],
    mysql_path: str,
    timeout: int,
    count_timeout: int = DEFAULT_COUNT_TIMEOUT,
    tag: str = "",
) -> Optional[BackupMetadata]:
    """Collect table statistics after a successful backup.

    Returns BackupMetadata on success, None on any failure.
    Never raises -- metadata collection must not break a successful backup.

    Steps:
      1. Query information_schema for TABLE_ROWS (estimate), DATA_LENGTH,
         INDEX_LENGTH in a single round-trip.
      2. For each table, run ``SELECT COUNT(*)`` for exact row count.
         If a single COUNT times out (count_timeout seconds), fall back
         to the estimate from step 1 and mark ``estimated=True``.
      3. For table-level backups, collect DDL via SHOW CREATE TABLE.
      4. Aggregate totals and attach ``backup_tag``.
    """
    mysql_bin = shutil.which(mysql_path)
    if not mysql_bin:
        return None

    try:
        db_escaped = _escape_sql_identifier(config.database)
        cmd_base = [
            mysql_bin,
            f"--host={config.host}",
            f"--port={config.port}",
            f"--user={config.user}",
            f"--default-character-set={config.charset}",
            "--batch",
            "--skip-column-names",
        ]
        env = {**os.environ, MYSQL_PWD_ENV: config.password}

        def _run_query(query: str, query_timeout: int = timeout) -> Optional[str]:
            """Run a mysql -e query. Returns stdout or None on failure."""
            proc = subprocess.run(
                [*cmd_base, "-e", query, config.database],
                capture_output=True, env=env, timeout=query_timeout,
            )
            if proc.returncode != 0:
                logger.warning("Metadata query failed: %s",
                               proc.stderr.decode("utf-8", errors="replace").strip())
                return None
            return proc.stdout.decode("utf-8", errors="replace").strip()

        # Step 1: information_schema for estimates + sizes (single query)
        info_query = (
            "SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH "
            "FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA = {db_escaped}"
        )
        if tables:
            quoted = ", ".join(_escape_sql_identifier(t) for t in tables)
            info_query += f" AND TABLE_NAME IN ({quoted})"
        info_query += " ORDER BY TABLE_NAME"

        info_output = _run_query(info_query)
        if info_output is None:
            return None

        # Parse: "table_name\test_rows\tdata_len\tindex_len\n"
        table_info: dict = {}  # name -> (est_rows, data_size, index_size)
        for line in info_output.splitlines():
            parts = line.split("\t")
            if len(parts) >= _INFO_COL_COUNT:
                tname = parts[_INFO_COL_NAME].strip()
                est_rows = _safe_int(parts[_INFO_COL_ROWS])
                data_sz = _safe_int(parts[_INFO_COL_DATA])
                index_sz = _safe_int(parts[_INFO_COL_INDEX])
                table_info[tname] = (est_rows, data_sz, index_sz)

        # Step 2: exact COUNT(*) per table (with per-table timeout fallback)
        exact_counts: dict = {}   # name -> (count, estimated)
        for tname in sorted(table_info):
            escaped = _escape_backtick_identifier(tname)
            try:
                count_output = _run_query(
                    f"SELECT COUNT(*) FROM {escaped}",
                    query_timeout=count_timeout,
                )
                if count_output is not None:
                    exact_counts[tname] = (_safe_int(count_output), False)
                else:
                    exact_counts[tname] = (table_info[tname][0], True)
            except subprocess.TimeoutExpired:
                logger.info("COUNT(*) timed out for %s, using estimate", tname)
                exact_counts[tname] = (table_info[tname][0], True)

        # Step 3: DDL for table-level backups
        ddl_map: dict = {}
        if tables:
            for tname in tables:
                escaped = _escape_backtick_identifier(tname)
                ddl_output = _run_query(f"SHOW CREATE TABLE {escaped}")
                if ddl_output is not None:
                    parts = ddl_output.split("\t", 1)
                    if len(parts) >= _DDL_COL_COUNT:
                        ddl_map[tname] = parts[_DDL_COL_VALUE].strip()

        # Step 4: build result
        stats = []
        total_data = 0
        total_index = 0
        for tname in sorted(table_info):
            est_rows, data_sz, index_sz = table_info[tname]
            row_count, estimated = exact_counts.get(tname, (est_rows, True))
            total_data += data_sz
            total_index += index_sz
            stats.append(TableStats(
                name=tname,
                row_count=row_count,
                estimated=estimated,
                data_size=data_sz,
                index_size=index_sz,
                ddl=ddl_map.get(tname, ""),
            ))

        return BackupMetadata(
            table_count=len(stats),
            table_stats=stats,
            total_data_size=total_data,
            total_index_size=total_index,
            backup_tag=tag,
        )

    except Exception:
        logger.warning("Metadata collection failed", exc_info=True)
        return None


def _safe_int(val: str) -> int:
    """Parse string to int, returning 0 on failure (e.g. 'NULL')."""
    try:
        return int(val.strip())
    except (ValueError, AttributeError):
        return 0


def _run_backup(
    *,
    config: ConnectionConfig,
    tables: Optional[List[str]],
    output_dir: str,
    compress: bool,
    timeout: int,
    mysqldump_path: str,
    mysql_path: str,
    extra_args: Sequence[str],
    collect_metadata: bool,
    count_timeout: int,
    tag: str,
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
                remaining = max(MIN_WAIT_TIMEOUT, timeout - int(time.monotonic() - start))
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                raise
            finally:
                stderr_t.join(STDERR_DRAIN_TIMEOUT)
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

        metadata = None
        if collect_metadata:
            metadata = _collect_metadata(
                config, tables, mysql_path=mysql_path, timeout=timeout,
                count_timeout=count_timeout, tag=tag,
            )

        # Auto-save metadata sidecar
        if metadata:
            save_backup_metadata(str(file_path), metadata)

        return BackupResult(
            success=True, file_path=str(file_path), file_size=file_size,
            duration=duration, tables=tables, metadata=metadata,
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
        return RestoreResult(success=False, file_path=backup_file,
                             error=f"Backup file not found: {backup_file}")

    mysql_bin = shutil.which(mysql_path)
    if not mysql_bin:
        return RestoreResult(success=False, file_path=backup_file,
                             error=f"mysql client not found: {mysql_path}")

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
                remaining = max(MIN_WAIT_TIMEOUT, timeout - int(time.monotonic() - start))
                proc.wait(timeout=remaining)
            except BrokenPipeError:
                logger.info("mysql closed stdin early (BrokenPipeError)")
                proc.wait()
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                raise
            finally:
                stderr_t.join(STDERR_DRAIN_TIMEOUT)
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
            return RestoreResult(success=False, file_path=backup_file,
                                 duration=duration, error=err)

        logger.info("%s: %s -> %s in %.1fs",
                     log_prefix, backup_file, config.database, duration)
        return RestoreResult(success=True, file_path=backup_file, duration=duration)

    except subprocess.TimeoutExpired:
        duration = time.monotonic() - start
        return RestoreResult(
            success=False, file_path=backup_file, duration=duration,
            error=f"{log_prefix} timed out after {timeout}s",
        )
    except Exception as exc:
        duration = time.monotonic() - start
        logger.exception("%s failed", log_prefix)
        return RestoreResult(success=False, file_path=backup_file,
                             duration=duration, error=str(exc))
