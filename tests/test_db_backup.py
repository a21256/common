"""Tests for yumoyi_common.db_backup — pure Python layer.

All subprocess calls are mocked; no real database connection needed.
"""

import gzip
import io
import os
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from yumoyi_common.db_backup import (
    ConnectionConfig,
    BackupResult,
    BackupMetadata,
    TableStats,
    RestoreResult,
    ListTablesResult,
    MYSQL_PWD_ENV,
    backup_database,
    backup_tables,
    restore_backup,
    restore_tables,
    list_tables,
    cleanup_old_backups,
    _table_suffix,
    _is_gzipped,
    _collect_metadata,
)


# ==================== Test fixtures & helpers ====================

CFG = ConnectionConfig(
    host="localhost",
    user="root",
    password="secret",
    database="testdb",
)


@pytest.fixture
def tmp_dir(tmp_path):
    return str(tmp_path)


def _mock_proc(returncode=0, stdout=b"-- SQL dump", stderr=b""):
    """Simple mock for subprocess.run return value (list_tables, etc.)."""
    proc = MagicMock()
    proc.returncode = returncode
    proc.stdout = stdout
    proc.stderr = stderr
    return proc


def _mock_run_streaming(stdout_data=b"-- SQL dump", returncode=0, stderr=b""):
    """Side effect for subprocess.run that writes to a stdout file object.

    Used for non-compressed backup tests where subprocess.run(stdout=file)
    is called.
    """
    def side_effect(cmd, **kwargs):
        target = kwargs.get("stdout")
        if target is not None and hasattr(target, "write"):
            target.write(stdout_data)
        proc = MagicMock()
        proc.returncode = returncode
        proc.stderr = stderr
        return proc
    return side_effect


def _mock_popen_result(stdout_data=b"", returncode=0, stderr_data=b""):
    """Create a mock Popen instance with BytesIO stdout/stderr."""
    mock_proc = MagicMock()
    mock_proc.stdout = io.BytesIO(stdout_data)
    mock_proc.stderr = io.BytesIO(stderr_data)
    mock_proc.returncode = returncode
    mock_proc.wait.return_value = returncode
    return mock_proc


# ==================== ConnectionConfig ====================

class TestConnectionConfig:
    def test_defaults(self):
        cfg = ConnectionConfig(host="h", user="u", password="p", database="d")
        assert cfg.port == 3306
        assert cfg.charset == "utf8mb4"

    def test_frozen(self):
        cfg = ConnectionConfig(host="h", user="u", password="p", database="d")
        with pytest.raises(AttributeError):
            cfg.host = "other"


# ==================== Backward compatibility ====================

class TestBackwardCompat:
    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_flat_params_still_work_with_warning(self, mock_run, mock_which, tmp_dir):
        mock_run.side_effect = _mock_run_streaming()
        with pytest.warns(DeprecationWarning, match="flat keyword"):
            result = backup_database(
                host="localhost", user="root", password="secret",
                database="testdb", output_dir=tmp_dir,
            )
        assert result.success is True

    def test_no_config_no_flat_raises_typeerror(self, tmp_dir):
        with pytest.raises(TypeError, match="Missing required"):
            backup_database(output_dir=tmp_dir)


# ==================== backup_database ====================

class TestBackupDatabase:
    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_success(self, mock_run, mock_which, tmp_dir):
        mock_run.side_effect = _mock_run_streaming(stdout_data=b"-- full dump")
        result = backup_database(config=CFG, output_dir=tmp_dir)

        assert result.success is True
        assert result.file_path != ""
        assert result.file_size > 0
        assert result.duration >= 0
        assert result.tables is None  # full backup -> None
        assert Path(result.file_path).exists()
        assert Path(result.file_path).read_bytes() == b"-- full dump"

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.Popen")
    def test_compressed(self, mock_popen, mock_which, tmp_dir):
        mock_popen.return_value = _mock_popen_result(stdout_data=b"-- compressed")
        result = backup_database(config=CFG, output_dir=tmp_dir, compress=True)

        assert result.success is True
        assert result.file_path.endswith(".sql.gz")
        with gzip.open(result.file_path, "rb") as gz:
            assert gz.read() == b"-- compressed"

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_mysqldump_fails(self, mock_run, mock_which, tmp_dir):
        mock_run.side_effect = _mock_run_streaming(
            stdout_data=b"", returncode=2, stderr=b"Access denied",
        )
        result = backup_database(config=CFG, output_dir=tmp_dir)

        assert result.success is False
        assert "Access denied" in result.error

    @patch("yumoyi_common.db_backup.shutil.which", return_value=None)
    def test_mysqldump_not_found(self, mock_which, tmp_dir):
        result = backup_database(config=CFG, output_dir=tmp_dir)

        assert result.success is False
        assert "not found" in result.error

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_creates_output_dir(self, mock_run, mock_which, tmp_path):
        mock_run.side_effect = _mock_run_streaming()
        nested = str(tmp_path / "a" / "b" / "c")
        result = backup_database(config=CFG, output_dir=nested)

        assert result.success is True
        assert Path(nested).is_dir()

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run",
           side_effect=subprocess.TimeoutExpired("cmd", 5))
    def test_timeout(self, mock_run, mock_which, tmp_dir):
        result = backup_database(config=CFG, output_dir=tmp_dir, timeout=5)

        assert result.success is False
        assert "timed out" in result.error

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_filename_format(self, mock_run, mock_which, tmp_dir):
        mock_run.side_effect = _mock_run_streaming()
        result = backup_database(config=CFG, output_dir=tmp_dir)

        filename = Path(result.file_path).name
        assert filename.startswith("testdb_")
        assert filename.endswith(".sql")

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_password_in_env_not_cmdline(self, mock_run, mock_which, tmp_dir):
        mock_run.side_effect = _mock_run_streaming()
        backup_database(config=CFG, output_dir=tmp_dir)

        # First call is mysqldump; subsequent calls are metadata collection
        dump_call = mock_run.call_args_list[0]
        cmd = dump_call[0][0]
        assert all("secret" not in arg for arg in cmd)
        assert dump_call[1]["env"][MYSQL_PWD_ENV] == "secret"

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_extra_args_passed(self, mock_run, mock_which, tmp_dir):
        mock_run.side_effect = _mock_run_streaming()
        backup_database(
            config=CFG, output_dir=tmp_dir,
            extra_args=["--column-statistics=0", "--set-gtid-purged=OFF"],
        )

        cmd = mock_run.call_args_list[0][0][0]
        assert "--column-statistics=0" in cmd
        assert "--set-gtid-purged=OFF" in cmd

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_streams_to_file_not_memory(self, mock_run, mock_which, tmp_dir):
        """Non-compressed backup uses stdout=file, not capture_output."""
        mock_run.side_effect = _mock_run_streaming()
        backup_database(config=CFG, output_dir=tmp_dir)

        dump_kwargs = mock_run.call_args_list[0][1]
        assert "stdout" in dump_kwargs
        assert hasattr(dump_kwargs["stdout"], "write")


# ==================== backup_tables ====================

class TestBackupTables:
    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_success(self, mock_run, mock_which, tmp_dir):
        mock_run.side_effect = _mock_run_streaming(stdout_data=b"-- table dump")
        result = backup_tables(
            config=CFG, tables=["users", "orders"], output_dir=tmp_dir,
        )

        assert result.success is True
        assert result.tables == ["orders", "users"]  # sorted
        cmd = mock_run.call_args_list[0][0][0]
        assert "users" in cmd
        assert "orders" in cmd

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_filename_single_table(self, mock_run, mock_which, tmp_dir):
        mock_run.side_effect = _mock_run_streaming()
        result = backup_tables(config=CFG, tables=["t1"], output_dir=tmp_dir)

        filename = Path(result.file_path).name
        assert filename.startswith("testdb_t1_")

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_filename_multiple_tables(self, mock_run, mock_which, tmp_dir):
        mock_run.side_effect = _mock_run_streaming()
        result = backup_tables(
            config=CFG, tables=["users", "orders"], output_dir=tmp_dir,
        )

        filename = Path(result.file_path).name
        # Sorted: orders before users
        assert filename.startswith("testdb_orders_users_")

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_filename_many_tables(self, mock_run, mock_which, tmp_dir):
        mock_run.side_effect = _mock_run_streaming()
        tables = ["t1", "t2", "t3", "t4"]
        result = backup_tables(config=CFG, tables=tables, output_dir=tmp_dir)

        filename = Path(result.file_path).name
        assert filename.startswith("testdb_4tables_")

    def test_empty_tables_returns_error(self, tmp_dir):
        result = backup_tables(config=CFG, tables=[], output_dir=tmp_dir)

        assert result.success is False
        assert "No tables" in result.error


# ==================== restore_backup ====================

class TestRestoreBackup:
    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_success_sql(self, mock_run, mock_which, tmp_path):
        backup = tmp_path / "backup.sql"
        backup.write_bytes(b"-- restore data")
        mock_run.return_value = _mock_proc(returncode=0)

        result = restore_backup(config=CFG, backup_file=str(backup))

        assert result.success is True
        assert result.duration >= 0
        # Verify streaming: stdin=file, not input=bytes
        call_kwargs = mock_run.call_args[1]
        assert "stdin" in call_kwargs
        assert "input" not in call_kwargs

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.Popen")
    def test_success_gzipped(self, mock_popen, mock_which, tmp_path):
        backup = tmp_path / "backup.sql.gz"
        with gzip.open(backup, "wb") as gz:
            gz.write(b"-- gzipped data")

        mock_proc = _mock_popen_result(returncode=0)
        mock_popen.return_value = mock_proc

        result = restore_backup(config=CFG, backup_file=str(backup))

        assert result.success is True
        written = b"".join(
            call.args[0] for call in mock_proc.stdin.write.call_args_list
        )
        assert written == b"-- gzipped data"

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_mysql_fails(self, mock_run, mock_which, tmp_path):
        backup = tmp_path / "backup.sql"
        backup.write_bytes(b"data")
        mock_run.return_value = _mock_proc(returncode=1, stderr=b"Unknown database")

        result = restore_backup(config=CFG, backup_file=str(backup))

        assert result.success is False
        assert "Unknown database" in result.error

    def test_file_not_found(self):
        result = restore_backup(config=CFG, backup_file="/nonexistent/backup.sql")

        assert result.success is False
        assert "not found" in result.error

    @patch("yumoyi_common.db_backup.shutil.which", return_value=None)
    def test_mysql_client_not_found(self, mock_which, tmp_path):
        backup = tmp_path / "backup.sql"
        backup.write_bytes(b"data")

        result = restore_backup(config=CFG, backup_file=str(backup))

        assert result.success is False
        assert "not found" in result.error

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_password_in_env(self, mock_run, mock_which, tmp_path):
        backup = tmp_path / "backup.sql"
        backup.write_bytes(b"data")
        mock_run.return_value = _mock_proc(returncode=0)

        restore_backup(config=CFG, backup_file=str(backup))

        env = mock_run.call_args[1]["env"]
        assert env[MYSQL_PWD_ENV] == "secret"

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_extra_args_passed(self, mock_run, mock_which, tmp_path):
        backup = tmp_path / "backup.sql"
        backup.write_bytes(b"data")
        mock_run.return_value = _mock_proc(returncode=0)

        restore_backup(config=CFG, backup_file=str(backup), extra_args=["--force"])

        cmd = mock_run.call_args[0][0]
        assert "--force" in cmd

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_plain_gz_not_treated_as_sql_gz(self, mock_run, mock_which, tmp_path):
        """A .csv.gz file is NOT treated as gzipped SQL."""
        backup = tmp_path / "data.csv.gz"
        backup.write_bytes(b"not sql")
        mock_run.return_value = _mock_proc(returncode=0)

        # Should go through the non-gzipped path (stdin=file)
        restore_backup(config=CFG, backup_file=str(backup))
        call_kwargs = mock_run.call_args[1]
        assert "stdin" in call_kwargs


# ==================== cleanup_old_backups ====================

class TestCleanupOldBackups:
    def test_keeps_recent(self, tmp_path):
        for i in range(7):
            f = tmp_path / f"testdb_{20250101 + i}_120000.sql"
            f.write_text("data")
            os.utime(f, (1000000 + i * 100, 1000000 + i * 100))

        deleted = cleanup_old_backups(
            output_dir=str(tmp_path), prefix="testdb", keep=3,
        )

        assert deleted == 4
        remaining = list(tmp_path.glob("testdb_*.sql"))
        assert len(remaining) == 3

    def test_nothing_to_delete(self, tmp_path):
        for i in range(2):
            (tmp_path / f"testdb_{20250101 + i}.sql").write_text("d")

        deleted = cleanup_old_backups(
            output_dir=str(tmp_path), prefix="testdb", keep=5,
        )
        assert deleted == 0

    def test_nonexistent_dir(self):
        deleted = cleanup_old_backups(
            output_dir="/nonexistent/path", prefix="x", keep=3,
        )
        assert deleted == 0

    def test_ignores_non_matching_files(self, tmp_path):
        (tmp_path / "testdb_20250101.sql").write_text("keep")
        (tmp_path / "other_file.txt").write_text("ignore")
        (tmp_path / "testdb_readme.md").write_text("ignore")

        deleted = cleanup_old_backups(
            output_dir=str(tmp_path), prefix="testdb", keep=5,
        )
        assert deleted == 0

    def test_handles_gzipped_files(self, tmp_path):
        for i in range(4):
            f = tmp_path / f"testdb_{20250101 + i}_120000.sql.gz"
            f.write_bytes(b"data")
            os.utime(f, (1000000 + i * 100, 1000000 + i * 100))

        deleted = cleanup_old_backups(
            output_dir=str(tmp_path), prefix="testdb", keep=2,
        )

        assert deleted == 2
        remaining = list(tmp_path.glob("testdb_*.sql.gz"))
        assert len(remaining) == 2

    def test_prefix_boundary_prevents_cross_match(self, tmp_path):
        """prefix='myapp' must NOT match 'myapp2_...' files."""
        (tmp_path / "myapp_20250101_120000.sql").write_text("mine")
        (tmp_path / "myapp2_20250101_120000.sql").write_text("not mine")
        os.utime(tmp_path / "myapp_20250101_120000.sql", (100, 100))
        os.utime(tmp_path / "myapp2_20250101_120000.sql", (200, 200))

        deleted = cleanup_old_backups(
            output_dir=str(tmp_path), prefix="myapp", keep=0,
        )

        assert deleted == 1
        assert not (tmp_path / "myapp_20250101_120000.sql").exists()
        assert (tmp_path / "myapp2_20250101_120000.sql").exists()

    def test_pattern_overrides_prefix(self, tmp_path):
        """Pattern parameter enables precise matching."""
        full = tmp_path / "testdb_20250101_120000.sql"
        table = tmp_path / "testdb_users_20250101_120000.sql"
        full.write_text("full")
        table.write_text("table")
        os.utime(full, (1000000, 1000000))
        os.utime(table, (1000001, 1000001))

        deleted = cleanup_old_backups(
            output_dir=str(tmp_path), prefix="testdb",
            pattern="testdb_[0-9]*.sql*", keep=0,
        )

        assert deleted == 1
        assert not full.exists()
        assert table.exists()  # table backup untouched

    def test_returns_actual_delete_count(self, tmp_path):
        """Return value reflects actual deletions."""
        f = tmp_path / "testdb_20250101.sql"
        f.write_text("data")

        deleted = cleanup_old_backups(
            output_dir=str(tmp_path), prefix="testdb", keep=0,
        )
        assert deleted == 1


# ==================== _table_suffix ====================

class TestTableSuffix:
    def test_no_tables(self):
        assert _table_suffix(None) == ""
        assert _table_suffix([]) == ""

    def test_single_table(self):
        assert _table_suffix(["users"]) == "_users"

    def test_two_tables(self):
        assert _table_suffix(["users", "orders"]) == "_orders_users"  # sorted

    def test_three_tables(self):
        assert _table_suffix(["c", "a", "b"]) == "_a_b_c"  # sorted

    def test_four_tables(self):
        assert _table_suffix(["a", "b", "c", "d"]) == "_4tables"

    def test_many_tables(self):
        assert _table_suffix(["t"] * 10) == "_10tables"

    def test_deterministic_order(self):
        """Same tables in different order produce the same suffix."""
        assert _table_suffix(["b", "a"]) == _table_suffix(["a", "b"])


# ==================== _is_gzipped ====================

class TestIsGzipped:
    def test_sql_gz(self):
        assert _is_gzipped(Path("backup.sql.gz")) is True

    def test_plain_sql(self):
        assert _is_gzipped(Path("backup.sql")) is False

    def test_other_gz_not_matched(self):
        """Only .sql.gz is recognized, not arbitrary .gz files."""
        assert _is_gzipped(Path("data.csv.gz")) is False


# ==================== restore_tables ====================

class TestRestoreTables:
    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_delegates_to_run_restore(self, mock_run, mock_which, tmp_path):
        backup = tmp_path / "testdb_users.sql"
        backup.write_bytes(b"-- table data")
        mock_run.return_value = _mock_proc(returncode=0)

        result = restore_tables(config=CFG, backup_file=str(backup))

        assert result.success is True
        mock_run.assert_called_once()

    def test_file_not_found(self):
        result = restore_tables(config=CFG, backup_file="/nonexistent.sql")
        assert result.success is False
        assert "not found" in result.error

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.Popen")
    def test_gzipped(self, mock_popen, mock_which, tmp_path):
        backup = tmp_path / "testdb_users.sql.gz"
        with gzip.open(backup, "wb") as gz:
            gz.write(b"-- gzipped table data")

        mock_proc = _mock_popen_result(returncode=0)
        mock_popen.return_value = mock_proc

        result = restore_tables(config=CFG, backup_file=str(backup))

        assert result.success is True
        written = b"".join(
            call.args[0] for call in mock_proc.stdin.write.call_args_list
        )
        assert written == b"-- gzipped table data"


# ==================== list_tables ====================

class TestListTables:
    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_success(self, mock_run, mock_which):
        mock_run.return_value = _mock_proc(
            returncode=0, stdout=b"orders\nproducts\nusers\n",
        )

        result = list_tables(config=CFG)

        assert result.success is True
        assert result.tables == ["orders", "products", "users"]
        cmd = mock_run.call_args[0][0]
        assert "--batch" in cmd
        assert "--skip-column-names" in cmd
        assert "SHOW TABLES" in cmd

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_empty_database(self, mock_run, mock_which):
        mock_run.return_value = _mock_proc(returncode=0, stdout=b"")

        result = list_tables(config=CFG)

        assert result.success is True
        assert result.tables == []

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_connection_error(self, mock_run, mock_which):
        mock_run.return_value = _mock_proc(returncode=1, stderr=b"Access denied")

        result = list_tables(config=CFG)

        assert result.success is False
        assert "Access denied" in result.error
        assert result.tables == []

    @patch("yumoyi_common.db_backup.shutil.which", return_value=None)
    def test_mysql_not_found(self, mock_which):
        result = list_tables(config=CFG)

        assert result.success is False
        assert "not found" in result.error

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_timeout(self, mock_run, mock_which):
        mock_run.side_effect = subprocess.TimeoutExpired("cmd", 5)

        result = list_tables(config=CFG, timeout=5)

        assert result.success is False
        assert "timed out" in result.error

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_password_in_env(self, mock_run, mock_which):
        mock_run.return_value = _mock_proc(returncode=0, stdout=b"t1\n")

        list_tables(config=CFG)

        cmd = mock_run.call_args[0][0]
        assert all("secret" not in arg for arg in cmd)
        assert mock_run.call_args[1]["env"][MYSQL_PWD_ENV] == "secret"

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_returns_sorted(self, mock_run, mock_which):
        mock_run.return_value = _mock_proc(
            returncode=0, stdout=b"zebra\nalpha\nmango\n",
        )

        result = list_tables(config=CFG)
        assert result.tables == ["alpha", "mango", "zebra"]


# ==================== ListTablesResult ====================

class TestListTablesResult:
    def test_success_vs_error_distinguishable(self):
        ok_empty = ListTablesResult(success=True, tables=[])
        err = ListTablesResult(success=False, error="Connection refused")

        assert ok_empty.success is True
        assert ok_empty.error == ""
        assert err.success is False
        assert err.error == "Connection refused"


# ==================== _collect_metadata ====================

# info_schema output format: "table_name\test_rows\tdata_len\tindex_len"
_INFO_3_TABLES = b"orders\t1234\t50000\t12000\nproducts\t567\t30000\t8000\nusers\t89\t10000\t2000\n"
_INFO_1_TABLE = b"users\t89\t10000\t2000\n"


class TestCollectMetadata:
    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_full_backup_exact_counts(self, mock_run, mock_which):
        """Full backup: info_schema + COUNT(*) per table, sizes populated."""
        mock_run.side_effect = [
            _mock_proc(returncode=0, stdout=_INFO_3_TABLES),  # info_schema
            _mock_proc(returncode=0, stdout=b"1300"),          # COUNT orders
            _mock_proc(returncode=0, stdout=b"600"),           # COUNT products
            _mock_proc(returncode=0, stdout=b"95"),            # COUNT users
        ]

        meta = _collect_metadata(CFG, tables=None, mysql_path="mysql", timeout=30)

        assert meta is not None
        assert meta.table_count == 3
        # Exact counts used
        assert meta.table_stats[0].name == "orders"
        assert meta.table_stats[0].row_count == 1300
        assert meta.table_stats[0].estimated is False
        # Sizes from info_schema
        assert meta.table_stats[0].data_size == 50000
        assert meta.table_stats[0].index_size == 12000
        # Totals
        assert meta.total_data_size == 90000
        assert meta.total_index_size == 22000

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_count_timeout_falls_back_to_estimate(self, mock_run, mock_which):
        """COUNT(*) timeout -> fall back to info_schema estimate, mark estimated=True."""
        mock_run.side_effect = [
            _mock_proc(returncode=0, stdout=_INFO_1_TABLE),       # info_schema
            subprocess.TimeoutExpired("cmd", 5),                   # COUNT times out
        ]

        meta = _collect_metadata(CFG, tables=None, mysql_path="mysql", timeout=30)

        assert meta is not None
        assert meta.table_stats[0].name == "users"
        assert meta.table_stats[0].row_count == 89      # fallback to estimate
        assert meta.table_stats[0].estimated is True

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_table_backup_with_ddl(self, mock_run, mock_which):
        """Table backup: exact counts + DDL."""
        mock_run.side_effect = [
            _mock_proc(returncode=0, stdout=_INFO_1_TABLE),                    # info_schema
            _mock_proc(returncode=0, stdout=b"95"),                            # COUNT users
            _mock_proc(returncode=0, stdout=b"users\tCREATE TABLE `users` (id INT)\n"),  # DDL
        ]

        meta = _collect_metadata(
            CFG, tables=["users"], mysql_path="mysql", timeout=30,
        )

        assert meta is not None
        assert meta.table_stats[0].row_count == 95
        assert meta.table_stats[0].estimated is False
        assert "CREATE TABLE" in meta.table_stats[0].ddl

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_backup_tag_stored(self, mock_run, mock_which):
        mock_run.side_effect = [
            _mock_proc(returncode=0, stdout=_INFO_1_TABLE),
            _mock_proc(returncode=0, stdout=b"95"),
        ]
        meta = _collect_metadata(
            CFG, tables=None, mysql_path="mysql", timeout=30,
            tag="pre_import_auto",
        )
        assert meta is not None
        assert meta.backup_tag == "pre_import_auto"

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_metadata_failure_returns_none(self, mock_run, mock_which):
        mock_run.return_value = _mock_proc(returncode=1, stderr=b"Access denied")
        meta = _collect_metadata(CFG, tables=None, mysql_path="mysql", timeout=30)
        assert meta is None

    @patch("yumoyi_common.db_backup.shutil.which", return_value=None)
    def test_mysql_not_found_returns_none(self, mock_which):
        meta = _collect_metadata(CFG, tables=None, mysql_path="mysql", timeout=30)
        assert meta is None

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_metadata_wired_into_backup(self, mock_run, mock_which, tmp_dir):
        """backup_database() populates result.metadata on success."""
        mock_run.side_effect = [
            # 1: mysqldump (streaming to file)
            MagicMock(returncode=0, stderr=b""),
            # 2: info_schema query
            _mock_proc(returncode=0, stdout=b"t1\t100\t5000\t1000\nt2\t200\t8000\t2000\n"),
            # 3: COUNT t1
            _mock_proc(returncode=0, stdout=b"105"),
            # 4: COUNT t2
            _mock_proc(returncode=0, stdout=b"210"),
        ]
        # Override first call to write to the stdout file
        original_side_effect = mock_run.side_effect
        calls = list(original_side_effect)

        def side_effect(cmd, **kwargs):
            idx = side_effect.call_idx
            side_effect.call_idx += 1
            if idx == 0:
                target = kwargs.get("stdout")
                if target and hasattr(target, "write"):
                    target.write(b"-- dump")
                return calls[0]
            return calls[idx]
        side_effect.call_idx = 0
        mock_run.side_effect = side_effect

        result = backup_database(config=CFG, output_dir=tmp_dir)

        assert result.success is True
        assert result.metadata is not None
        assert result.metadata.table_count == 2
        assert result.metadata.table_stats[0].row_count == 105
        assert result.metadata.table_stats[0].estimated is False
        assert result.metadata.total_data_size == 13000
