# yumoyi-common

Shared Python utilities for yumoyi projects.

## Modules

- **column_inference** — Excel column auto-inference engine (header keywords + data format analysis)
- **db_backup** — Database/table backup utilities (TODO)

## Install

```bash
# Editable install (dev)
pip install -e c:\yumoyi\common

# From GitHub
pip install git+https://github.com/a21256/common.git
```

## Usage

```python
from yumoyi_common.column_inference import FieldSpec, infer_columns, is_numeric

fields = [
    FieldSpec("name", required=True, keywords=("姓名", "Name")),
    FieldSpec("amount", keywords=("金额",), format_test=is_numeric),
]
mapping = infer_columns(ws, fields)
```

## Dev

```bash
pip install -e ".[dev]"
pytest tests/
```
