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
from yumoyi_common.column_inference import FieldSpec, infer_columns, is_numeric

fields = [
    FieldSpec("name", required=True, keywords=("姓名", "Name")),
    FieldSpec("amount", keywords=("金额",), format_test=is_numeric),
]
mapping = infer_columns(ws, fields)
```

### Database Backup

```python
from yumoyi_common.db_backup import ConnectionConfig, backup_database

config = ConnectionConfig(host="localhost", user="root", password="pwd", database="mydb")
result = backup_database(config=config, output_dir="/backups", compress=True)
```

### Django Management Commands

```bash
python manage.py dbbackup --output-dir /backups
python manage.py dbbackup --output-dir /backups --tables users orders --compress
python manage.py dbbackup --list-tables
python manage.py dbrestore /backups/mydb_20250101_120000.sql
```

## Dev

```bash
pip install -e ".[dev,django]"
pytest tests/
```
