"""Tests for yumoyi_common.django_db_backup — Django integration layer.

All database operations and subprocess calls are mocked.
"""

import io
from unittest.mock import patch, MagicMock

import pytest

from yumoyi_common.db_backup import (
    ConnectionConfig,
    BackupResult,
    BackupMetadata,
    TableStats,
    RestoreResult,
    ListTablesResult,
)


# ==================== Mock Django settings ====================

class MockSettings:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": "myapp_db",
            "USER": "dbuser",
            "PASSWORD": "dbpass",
            "HOST": "db.local",
            "PORT": "3307",
            "OPTIONS": {"charset": "utf8mb4"},
        },
        "sqlite_db": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": ":memory:",
        },
    }


SETTINGS_PATCH = "yumoyi_common.django_db_backup.settings"


# ==================== _get_connection_config ====================

class TestGetConnectionConfig:
    @patch(SETTINGS_PATCH, MockSettings)
    def test_extracts_mysql_params(self):
        from yumoyi_common.django_db_backup import _get_connection_config
        cfg = _get_connection_config("default")

        assert isinstance(cfg, ConnectionConfig)
        assert cfg.host == "db.local"
        assert cfg.port == 3307
        assert cfg.user == "dbuser"
        assert cfg.password == "dbpass"
        assert cfg.database == "myapp_db"
        assert cfg.charset == "utf8mb4"

    @patch(SETTINGS_PATCH, MockSettings)
    def test_rejects_non_mysql(self):
        from yumoyi_common.django_db_backup import _get_connection_config
        with pytest.raises(ValueError, match="only MySQL"):
            _get_connection_config("sqlite_db")

    @patch(SETTINGS_PATCH, MockSettings)
    def test_unknown_alias_raises_with_hint(self):
        from yumoyi_common.django_db_backup import _get_connection_config
        with pytest.raises(ValueError, match="not found.*Available"):
            _get_connection_config("nonexistent")


# ==================== backup_current_database ====================

class TestBackupCurrentDatabase:
    @patch(SETTINGS_PATCH, MockSettings)
    @patch("yumoyi_common.django_db_backup.backup_database")
    @patch("yumoyi_common.django_db_backup.get_migration_state",
           return_value="[X] 0001_initial")
    def test_full_backup(self, mock_migrate, mock_backup, tmp_path):
        mock_backup.return_value = BackupResult(
            success=True, file_path="/backups/myapp_db.sql",
            file_size=1024, duration=1.5,
        )
        from yumoyi_common.django_db_backup import backup_current_database
        result = backup_current_database(output_dir=str(tmp_path))

        assert result.success is True
        assert result.migration_state == "[X] 0001_initial"
        mock_backup.assert_called_once()
        call_kw = mock_backup.call_args[1]
        assert isinstance(call_kw["config"], ConnectionConfig)
        assert call_kw["config"].host == "db.local"
        assert call_kw["config"].database == "myapp_db"

    @patch(SETTINGS_PATCH, MockSettings)
    @patch("yumoyi_common.django_db_backup.backup_tables")
    @patch("yumoyi_common.django_db_backup.get_migration_state", return_value="")
    def test_table_backup(self, mock_migrate, mock_backup, tmp_path):
        mock_backup.return_value = BackupResult(
            success=True, file_path="/backups/myapp_db_users.sql",
            file_size=512, duration=0.8, tables=["users"],
        )
        from yumoyi_common.django_db_backup import backup_current_database
        result = backup_current_database(
            output_dir=str(tmp_path), tables=["users"],
        )

        assert result.success is True
        mock_backup.assert_called_once()
        assert mock_backup.call_args[1]["tables"] == ["users"]

    @patch(SETTINGS_PATCH, MockSettings)
    @patch("yumoyi_common.django_db_backup.backup_database")
    @patch("yumoyi_common.django_db_backup.get_migration_state", return_value="")
    def test_passes_compress_flag(self, mock_migrate, mock_backup, tmp_path):
        mock_backup.return_value = BackupResult(success=True)
        from yumoyi_common.django_db_backup import backup_current_database
        backup_current_database(output_dir=str(tmp_path), compress=True)

        assert mock_backup.call_args[1]["compress"] is True


# ==================== restore_to_current_database ====================

class TestRestoreToCurrentDatabase:
    @patch(SETTINGS_PATCH, MockSettings)
    @patch("yumoyi_common.django_db_backup.restore_backup")
    def test_restore(self, mock_restore, tmp_path):
        mock_restore.return_value = RestoreResult(success=True, duration=2.0)
        backup = tmp_path / "backup.sql"
        backup.write_text("-- data")

        from yumoyi_common.django_db_backup import restore_to_current_database
        result = restore_to_current_database(backup_file=str(backup))

        assert result.success is True
        call_kw = mock_restore.call_args[1]
        assert call_kw["config"].host == "db.local"
        assert call_kw["config"].database == "myapp_db"


# ==================== cleanup_current_database_backups ====================

class TestCleanupCurrentDatabaseBackups:
    @patch(SETTINGS_PATCH, MockSettings)
    @patch("yumoyi_common.django_db_backup.cleanup_old_backups", return_value=3)
    def test_cleanup(self, mock_cleanup, tmp_path):
        from yumoyi_common.django_db_backup import cleanup_current_database_backups
        deleted = cleanup_current_database_backups(
            output_dir=str(tmp_path), keep=5,
        )

        assert deleted == 3
        mock_cleanup.assert_called_once_with(
            output_dir=str(tmp_path), prefix="myapp_db", keep=5, pattern=None,
        )


# ==================== restore_tables_to_current_database ====================

class TestRestoreTablesToCurrentDatabase:
    @patch(SETTINGS_PATCH, MockSettings)
    @patch("yumoyi_common.django_db_backup.restore_tables")
    def test_restore_tables(self, mock_restore, tmp_path):
        mock_restore.return_value = RestoreResult(success=True, duration=1.0)
        backup = tmp_path / "myapp_db_users_20250101.sql"
        backup.write_text("-- table data")

        from yumoyi_common.django_db_backup import restore_tables_to_current_database
        result = restore_tables_to_current_database(backup_file=str(backup))

        assert result.success is True
        call_kw = mock_restore.call_args[1]
        assert call_kw["config"].host == "db.local"
        assert call_kw["config"].database == "myapp_db"
        assert call_kw["backup_file"] == str(backup)


# ==================== list_current_database_tables ====================

class TestListCurrentDatabaseTables:
    @patch(SETTINGS_PATCH, MockSettings)
    @patch("yumoyi_common.django_db_backup.list_tables")
    def test_success(self, mock_list):
        mock_list.return_value = ListTablesResult(
            success=True, tables=["auth_user", "orders", "products"],
        )

        from yumoyi_common.django_db_backup import list_current_database_tables
        result = list_current_database_tables()

        assert result.success is True
        assert result.tables == ["auth_user", "orders", "products"]
        call_kw = mock_list.call_args[1]
        assert call_kw["config"].host == "db.local"
        assert call_kw["config"].database == "myapp_db"

    @patch(SETTINGS_PATCH, MockSettings)
    @patch("yumoyi_common.django_db_backup.list_tables")
    def test_empty(self, mock_list):
        mock_list.return_value = ListTablesResult(success=True, tables=[])

        from yumoyi_common.django_db_backup import list_current_database_tables
        result = list_current_database_tables()

        assert result.success is True
        assert result.tables == []

    @patch(SETTINGS_PATCH, MockSettings)
    @patch("yumoyi_common.django_db_backup.list_tables")
    def test_rejects_non_mysql_alias(self, mock_list):
        mock_list.return_value = ListTablesResult(success=True, tables=["t1"])

        from yumoyi_common.django_db_backup import list_current_database_tables

        with pytest.raises(ValueError, match="only MySQL"):
            list_current_database_tables(db_alias="sqlite_db")


# ==================== Management command: dbbackup ====================

class TestDbbackupArgparse:
    """Validate that argparse definitions match what handle() expects."""

    def test_backup_flags_parsed(self):
        from yumoyi_common.management.commands.dbbackup import Command
        parser = Command().create_parser("manage.py", "dbbackup")
        opts = vars(parser.parse_args([
            "--output-dir", "/backups",
            "--mysqldump-path", "/usr/local/bin/mysqldump",
            "--mysql-path", "/usr/local/bin/mysql",
            "--compress",
            "--cleanup", "5",
        ]))
        assert opts["output_dir"] == "/backups"
        assert opts["mysqldump_path"] == "/usr/local/bin/mysqldump"
        assert opts["mysql_path"] == "/usr/local/bin/mysql"
        assert opts["compress"] is True
        assert opts["cleanup"] == 5

    def test_list_tables_flag_parsed(self):
        from yumoyi_common.management.commands.dbbackup import Command
        parser = Command().create_parser("manage.py", "dbbackup")
        opts = vars(parser.parse_args(["--list-tables"]))
        assert opts["list_tables"] is True
        assert opts["output_dir"] is None  # not required for --list-tables


class TestDrestoreArgparse:
    """Validate that argparse definitions match what handle() expects."""

    def test_restore_flags_parsed(self):
        from yumoyi_common.management.commands.dbrestore import Command
        parser = Command().create_parser("manage.py", "dbrestore")
        opts = vars(parser.parse_args([
            "/backups/dump.sql",
            "--mysql-path", "/usr/local/bin/mysql",
        ]))
        assert opts["backup_file"] == "/backups/dump.sql"
        assert opts["mysql_path"] == "/usr/local/bin/mysql"


class TestDbbackupCommand:
    @patch("yumoyi_common.management.commands.dbbackup.list_current_database_tables")
    def test_list_tables_success(self, mock_list):
        mock_list.return_value = ListTablesResult(
            success=True, tables=["auth_user", "orders"],
        )

        from yumoyi_common.management.commands.dbbackup import Command
        out = io.StringIO()
        cmd = Command(stdout=out)

        cmd.handle(
            list_tables=True, database="default",
            output_dir=None, tables=None, compress=False, cleanup=0,
            mysqldump_path="mysqldump", mysql_path="mysql",
        )

        output = out.getvalue()
        assert "auth_user" in output
        assert "orders" in output
        assert "2" in output

    @patch("yumoyi_common.management.commands.dbbackup.list_current_database_tables")
    def test_list_tables_error_raises(self, mock_list):
        mock_list.return_value = ListTablesResult(
            success=False, error="Connection refused",
        )

        from django.core.management.base import CommandError
        from yumoyi_common.management.commands.dbbackup import Command
        out = io.StringIO()
        cmd = Command(stdout=out)

        with pytest.raises(CommandError, match="Connection refused"):
            cmd.handle(
                list_tables=True, database="default",
                output_dir=None, tables=None, compress=False, cleanup=0,
                mysqldump_path="mysqldump", mysql_path="mysql",
            )

    @patch("yumoyi_common.management.commands.dbbackup.backup_current_database")
    def test_backup_success(self, mock_backup):
        mock_backup.return_value = BackupResult(
            success=True, file_path="/backups/test.sql",
            file_size=1024, duration=1.5,
        )

        from yumoyi_common.management.commands.dbbackup import Command
        out = io.StringIO()
        cmd = Command(stdout=out)
        cmd.style.SUCCESS = lambda x: x

        cmd.handle(
            list_tables=False, database="default",
            output_dir="/backups", tables=None, compress=False, cleanup=0,
            mysqldump_path="mysqldump", mysql_path="mysql",
        )

        output = out.getvalue()
        assert "/backups/test.sql" in output
        assert "1024 bytes" in output

    @patch("yumoyi_common.management.commands.dbbackup.backup_current_database")
    def test_backup_failure_raises(self, mock_backup):
        mock_backup.return_value = BackupResult(
            success=False, error="Access denied",
        )

        from django.core.management.base import CommandError
        from yumoyi_common.management.commands.dbbackup import Command
        out = io.StringIO()
        cmd = Command(stdout=out)

        with pytest.raises(CommandError, match="Access denied"):
            cmd.handle(
                list_tables=False, database="default",
                output_dir="/backups", tables=None, compress=False, cleanup=0,
                mysqldump_path="mysqldump", mysql_path="mysql",
            )

    def test_missing_output_dir_raises(self):
        from django.core.management.base import CommandError
        from yumoyi_common.management.commands.dbbackup import Command
        out = io.StringIO()
        cmd = Command(stdout=out)

        with pytest.raises(CommandError, match="--output-dir"):
            cmd.handle(
                list_tables=False, database="default",
                output_dir=None, tables=None, compress=False, cleanup=0,
            )

    @patch("yumoyi_common.management.commands.dbbackup.cleanup_current_database_backups",
           return_value=3)
    @patch("yumoyi_common.management.commands.dbbackup.backup_current_database")
    def test_backup_with_cleanup(self, mock_backup, mock_cleanup):
        mock_backup.return_value = BackupResult(
            success=True, file_path="/backups/test.sql",
            file_size=100, duration=0.5,
        )

        from yumoyi_common.management.commands.dbbackup import Command
        out = io.StringIO()
        cmd = Command(stdout=out)
        cmd.style.SUCCESS = lambda x: x

        cmd.handle(
            list_tables=False, database="default",
            output_dir="/backups", tables=None, compress=False, cleanup=5,
            mysqldump_path="mysqldump", mysql_path="mysql",
        )

        mock_cleanup.assert_called_once()
        assert "3" in out.getvalue()

    @patch("yumoyi_common.management.commands.dbbackup.backup_current_database")
    def test_backup_prints_metadata_summary(self, mock_backup):
        mock_backup.return_value = BackupResult(
            success=True, file_path="/backups/test.sql",
            file_size=1024, duration=1.0,
            metadata=BackupMetadata(
                table_count=2,
                table_stats=[
                    TableStats(name="orders", row_count=5000),
                    TableStats(name="users", row_count=123),
                ],
            ),
        )

        from yumoyi_common.management.commands.dbbackup import Command
        out = io.StringIO()
        cmd = Command(stdout=out)
        cmd.style.SUCCESS = lambda x: x

        cmd.handle(
            list_tables=False, database="default",
            output_dir="/backups", tables=None, compress=False, cleanup=0,
            mysqldump_path="mysqldump", mysql_path="mysql",
        )

        output = out.getvalue()
        assert "Tables: 2" in output
        assert "orders" in output
        assert "5,000" in output
        assert "users" in output
        assert "123" in output


# ==================== Management command: dbrestore ====================

class TestDrestoreCommand:
    @patch("yumoyi_common.management.commands.dbrestore.restore_to_current_database")
    def test_restore_success(self, mock_restore):
        mock_restore.return_value = RestoreResult(success=True, duration=2.0)

        from yumoyi_common.management.commands.dbrestore import Command
        out = io.StringIO()
        cmd = Command(stdout=out)
        cmd.style.SUCCESS = lambda x: x

        cmd.handle(backup_file="/backups/test.sql", database="default", mysql_path="mysql")

        assert "2.0s" in out.getvalue()

    @patch("yumoyi_common.management.commands.dbrestore.restore_to_current_database")
    def test_restore_failure_raises(self, mock_restore):
        mock_restore.return_value = RestoreResult(
            success=False, error="Table doesn't exist",
        )

        from django.core.management.base import CommandError
        from yumoyi_common.management.commands.dbrestore import Command
        out = io.StringIO()
        cmd = Command(stdout=out)

        with pytest.raises(CommandError, match="Table doesn't exist"):
            cmd.handle(backup_file="/backups/test.sql", database="default", mysql_path="mysql")
