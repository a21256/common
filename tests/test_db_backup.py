"""Tests for yumoyi_common.db_backup — pure Python layer.

All subprocess calls are mocked; no real database connection needed.
"""

import gzip
import os
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from yumoyi_common.db_backup import (
    backup_database,
    backup_tables,
    restore_backup,
    cleanup_old_backups,
    BackupResult,
    RestoreResult,
)


# ==================== Fixtures ====================

@pytest.fixture
def tmp_dir(tmp_path):
    return str(tmp_path)


DB_PARAMS = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "secret",
    "database": "testdb",
}


def _mock_proc(returncode=0, stdout=b"-- SQL dump", stderr=b""):
    proc = MagicMock()
    proc.returncode = returncode
    proc.stdout = stdout
    proc.stderr = stderr
    return proc


# ==================== backup_database ====================

class TestBackupDatabase:
    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_success(self, mock_run, mock_which, tmp_dir):
        mock_run.return_value = _mock_proc(stdout=b"-- full dump data")
        result = backup_database(**DB_PARAMS, output_dir=tmp_dir)

        assert result.success is True
        assert result.file_path != ""
        assert result.file_size > 0
        assert result.duration >= 0
        assert Path(result.file_path).exists()
        assert Path(result.file_path).read_bytes() == b"-- full dump data"

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_compressed(self, mock_run, mock_which, tmp_dir):
        mock_run.return_value = _mock_proc(stdout=b"-- compressed dump")
        result = backup_database(**DB_PARAMS, output_dir=tmp_dir, compress=True)

        assert result.success is True
        assert result.file_path.endswith(".sql.gz")
        with gzip.open(result.file_path, "rb") as gz:
            assert gz.read() == b"-- compressed dump"

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_mysqldump_fails(self, mock_run, mock_which, tmp_dir):
        mock_run.return_value = _mock_proc(returncode=2, stderr=b"Access denied")
        result = backup_database(**DB_PARAMS, output_dir=tmp_dir)

        assert result.success is False
        assert "Access denied" in result.error

    @patch("yumoyi_common.db_backup.shutil.which", return_value=None)
    def test_mysqldump_not_found(self, mock_which, tmp_dir):
        result = backup_database(**DB_PARAMS, output_dir=tmp_dir)

        assert result.success is False
        assert "not found" in result.error

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_creates_output_dir(self, mock_run, mock_which, tmp_path):
        mock_run.return_value = _mock_proc()
        nested = str(tmp_path / "a" / "b" / "c")
        result = backup_database(**DB_PARAMS, output_dir=nested)

        assert result.success is True
        assert Path(nested).is_dir()

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run", side_effect=subprocess.TimeoutExpired("cmd", 5))
    def test_timeout(self, mock_run, mock_which, tmp_dir):
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired("cmd", 5)
        result = backup_database(**DB_PARAMS, output_dir=tmp_dir, timeout=5)

        assert result.success is False
        assert "timed out" in result.error

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_filename_format(self, mock_run, mock_which, tmp_dir):
        mock_run.return_value = _mock_proc()
        result = backup_database(**DB_PARAMS, output_dir=tmp_dir)

        filename = Path(result.file_path).name
        assert filename.startswith("testdb_")
        assert filename.endswith(".sql")

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_password_in_env_not_cmdline(self, mock_run, mock_which, tmp_dir):
        mock_run.return_value = _mock_proc()
        backup_database(**DB_PARAMS, output_dir=tmp_dir)

        call_args = mock_run.call_args
        cmd = call_args[0][0]
        # Password should NOT be in command line arguments
        assert all("secret" not in arg for arg in cmd)
        # Password should be in environment
        assert call_args[1]["env"]["MYSQL_PWD"] == "secret"


# ==================== backup_tables ====================

class TestBackupTables:
    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_success(self, mock_run, mock_which, tmp_dir):
        mock_run.return_value = _mock_proc(stdout=b"-- table dump")
        result = backup_tables(**DB_PARAMS, tables=["users", "orders"], output_dir=tmp_dir)

        assert result.success is True
        assert result.tables == ["users", "orders"]
        # Tables should be in command line
        cmd = mock_run.call_args[0][0]
        assert "users" in cmd
        assert "orders" in cmd

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysqldump")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_filename_has_tables_suffix(self, mock_run, mock_which, tmp_dir):
        mock_run.return_value = _mock_proc()
        result = backup_tables(**DB_PARAMS, tables=["t1"], output_dir=tmp_dir)

        assert "_tables_" in Path(result.file_path).name

    def test_empty_tables_returns_error(self, tmp_dir):
        result = backup_tables(**DB_PARAMS, tables=[], output_dir=tmp_dir)

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

        result = restore_backup(**DB_PARAMS, backup_file=str(backup))

        assert result.success is True
        assert result.duration >= 0

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_success_gzipped(self, mock_run, mock_which, tmp_path):
        backup = tmp_path / "backup.sql.gz"
        with gzip.open(backup, "wb") as gz:
            gz.write(b"-- gzipped data")
        mock_run.return_value = _mock_proc(returncode=0)

        result = restore_backup(**DB_PARAMS, backup_file=str(backup))

        assert result.success is True
        # Verify decompressed data was passed to mysql
        call_input = mock_run.call_args[1]["input"]
        assert call_input == b"-- gzipped data"

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_mysql_fails(self, mock_run, mock_which, tmp_path):
        backup = tmp_path / "backup.sql"
        backup.write_bytes(b"data")
        mock_run.return_value = _mock_proc(returncode=1, stderr=b"Unknown database")

        result = restore_backup(**DB_PARAMS, backup_file=str(backup))

        assert result.success is False
        assert "Unknown database" in result.error

    def test_file_not_found(self):
        result = restore_backup(**DB_PARAMS, backup_file="/nonexistent/backup.sql")

        assert result.success is False
        assert "not found" in result.error

    @patch("yumoyi_common.db_backup.shutil.which", return_value=None)
    def test_mysql_client_not_found(self, mock_which, tmp_path):
        backup = tmp_path / "backup.sql"
        backup.write_bytes(b"data")

        result = restore_backup(**DB_PARAMS, backup_file=str(backup))

        assert result.success is False
        assert "not found" in result.error

    @patch("yumoyi_common.db_backup.shutil.which", return_value="/usr/bin/mysql")
    @patch("yumoyi_common.db_backup.subprocess.run")
    def test_password_in_env(self, mock_run, mock_which, tmp_path):
        backup = tmp_path / "backup.sql"
        backup.write_bytes(b"data")
        mock_run.return_value = _mock_proc(returncode=0)

        restore_backup(**DB_PARAMS, backup_file=str(backup))

        env = mock_run.call_args[1]["env"]
        assert env["MYSQL_PWD"] == "secret"


# ==================== cleanup_old_backups ====================

class TestCleanupOldBackups:
    def test_keeps_recent(self, tmp_path):
        for i in range(7):
            f = tmp_path / f"testdb_{20250101 + i}_120000.sql"
            f.write_text("data")
            # Ensure different mtimes
            os.utime(f, (1000000 + i * 100, 1000000 + i * 100))

        deleted = cleanup_old_backups(output_dir=str(tmp_path), prefix="testdb", keep=3)

        assert deleted == 4
        remaining = list(tmp_path.glob("testdb_*.sql"))
        assert len(remaining) == 3

    def test_nothing_to_delete(self, tmp_path):
        for i in range(2):
            (tmp_path / f"testdb_{20250101 + i}.sql").write_text("d")

        deleted = cleanup_old_backups(output_dir=str(tmp_path), prefix="testdb", keep=5)

        assert deleted == 0

    def test_nonexistent_dir(self):
        deleted = cleanup_old_backups(output_dir="/nonexistent/path", prefix="x", keep=3)
        assert deleted == 0

    def test_ignores_non_matching_files(self, tmp_path):
        (tmp_path / "testdb_20250101.sql").write_text("keep")
        (tmp_path / "other_file.txt").write_text("ignore")
        (tmp_path / "testdb_readme.md").write_text("ignore")

        deleted = cleanup_old_backups(output_dir=str(tmp_path), prefix="testdb", keep=5)
        assert deleted == 0

    def test_handles_gzipped_files(self, tmp_path):
        for i in range(4):
            f = tmp_path / f"testdb_{20250101 + i}_120000.sql.gz"
            f.write_bytes(b"data")
            os.utime(f, (1000000 + i * 100, 1000000 + i * 100))

        deleted = cleanup_old_backups(output_dir=str(tmp_path), prefix="testdb", keep=2)

        assert deleted == 2
        remaining = list(tmp_path.glob("testdb_*.sql.gz"))
        assert len(remaining) == 2
