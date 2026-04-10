# yumoyi-common

Shared Python utilities for yumoyi projects.

## Modules

- **column_inference** -- Excel column auto-inference engine (header keywords + data format analysis)
- **db_backup** -- MySQL backup/restore via mysqldump CLI (streaming, compression, cleanup)
- **django_db_backup** -- Django integration layer (auto-read settings, management commands)

## Install

```bash
# Editable install (dev)
pip install -e c:\yumoyi\common

# With Django support
pip install -e "c:\yumoyi\common[django]"

# From GitHub
pip install git+https://github.com/a21256/common.git
```

In `requirements.txt`:

```
yumoyi-common @ git+https://github.com/a21256/common.git
```

## Usage

### Column Inference

```python
from yumoyi_common.column_inference import FieldSpec, infer_columns, is_numeric, is_date_like

fields = [
    FieldSpec("name", required=True, keywords=("Name", "Full Name")),
    FieldSpec("amount", keywords=("Amount", "Total"), format_test=is_numeric),
    FieldSpec("date", keywords=("Date", "Order Date"), format_test=is_date_like),
]
mapping = infer_columns(ws, fields)
# mapping = {"name": 1, "amount": 3, "date": 4}  (1-based column indices)
```

### Database Backup (pure Python, no Django)

```python
from yumoyi_common.db_backup import (
    ConnectionConfig, backup_database, backup_tables,
    restore_backup, list_tables, cleanup_old_backups,
)

config = ConnectionConfig(host="localhost", user="root", password="pwd", database="mydb")

# Full database backup (streaming, never loads full dump into memory)
result = backup_database(config=config, output_dir="/backups", compress=True)
# result.success, result.file_path, result.file_size, result.duration

# Specific tables
result = backup_tables(config=config, tables=["users", "orders"], output_dir="/backups")

# List tables (ListTablesResult distinguishes empty DB from connection error)
lt = list_tables(config=config)
# lt.success, lt.tables, lt.error

# Restore
restore_backup(config=config, backup_file="/backups/mydb_20250101_120000.sql")

# Cleanup old backups, keep most recent 5
cleanup_old_backups(output_dir="/backups", prefix="mydb", keep=5)
```

Custom mysqldump/mysql path (e.g. non-standard install):

```python
backup_database(config=config, output_dir="/backups", mysqldump_path="/usr/local/mysql/bin/mysqldump")
restore_backup(config=config, backup_file="dump.sql", mysql_path="/usr/local/mysql/bin/mysql")
```

### Django Integration

```python
from yumoyi_common.django_db_backup import (
    backup_current_database, restore_to_current_database,
    list_current_database_tables,
)

# Reads connection params from settings.DATABASES automatically
result = backup_current_database(output_dir="/backups")
# result.migration_state contains Django migration snapshot at backup time
```

### Django Management Commands

```bash
python manage.py dbbackup --output-dir /backups
python manage.py dbbackup --output-dir /backups --tables users orders --compress
python manage.py dbbackup --output-dir /backups --mysqldump-path /usr/local/bin/mysqldump
python manage.py dbbackup --list-tables
python manage.py dbbackup --list-tables --mysql-path /usr/local/bin/mysql
python manage.py dbrestore /backups/mydb_20250101_120000.sql
python manage.py dbrestore dump.sql --mysql-path /usr/local/bin/mysql
```

## Dev

```bash
pip install -e ".[dev,django]"
pytest tests/
```
