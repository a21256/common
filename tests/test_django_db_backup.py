"""Tests for yumoyi_common.django_db_backup — Django integration layer.

All database operations and subprocess calls are mocked.
"""

from unittest.mock import patch, MagicMock

import pytest

from yumoyi_common.db_backup import BackupResult, RestoreResult


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


# We patch django.conf.settings at the module level for each test
SETTINGS_PATCH = "yumoyi_common.django_db_backup.settings"


# ==================== _get_db_params ====================

class TestGetDbParams:
    @patch(SETTINGS_PATCH, MockSettings)
    def test_extracts_mysql_params(self):
        from yumoyi_common.django_db_backup import _get_db_params
        params = _get_db_params("default")

        assert params["host"] == "db.local"
        assert params["port"] == 3307
        assert params["user"] == "dbuser"
        assert params["password"] == "dbpass"
        assert params["database"] == "myapp_db"
        assert params["charset"] == "utf8mb4"

    @patch(SETTINGS_PATCH, MockSettings)
    def test_rejects_non_mysql(self):
        from yumoyi_common.django_db_backup import _get_db_params
        with pytest.raises(ValueError, match="only MySQL"):
            _get_db_params("sqlite_db")


# ==================== backup_current_database ====================

class TestBackupCurrentDatabase:
    @patch(SETTINGS_PATCH, MockSettings)
    @patch("yumoyi_common.django_db_backup.backup_database")
    @patch("yumoyi_common.django_db_backup.get_migration_state", return_value="[X] 0001_initial")
    def test_full_backup(self, mock_migrate, mock_backup, tmp_path):
        mock_backup.return_value = BackupResult(
            success=True, file_path="/backups/myapp_db.sql",
            file_size=1024, duration=1.5,
        )
        from yumoyi_common.django_db_backup import backup_current_database
        result = backup_current_database(output_dir=str(tmp_path))

        assert result.success is True
        mock_backup.assert_called_once()
        call_kw = mock_backup.call_args[1]
        assert call_kw["host"] == "db.local"
        assert call_kw["database"] == "myapp_db"

    @patch(SETTINGS_PATCH, MockSettings)
    @patch("yumoyi_common.django_db_backup.backup_tables")
    @patch("yumoyi_common.django_db_backup.get_migration_state", return_value="")
    def test_table_backup(self, mock_migrate, mock_backup, tmp_path):
        mock_backup.return_value = BackupResult(
            success=True, file_path="/backups/myapp_db_tables.sql",
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
        assert call_kw["host"] == "db.local"
        assert call_kw["database"] == "myapp_db"


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
            output_dir=str(tmp_path), prefix="myapp_db", keep=5,
        )
