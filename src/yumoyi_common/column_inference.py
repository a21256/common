"""
Column auto-inference engine (generic, reusable across projects).

Given an openpyxl worksheet, infers which column maps to which logical field
by combining:
  1. Content format analysis (highest priority)
  2. Header keyword matching (secondary)

Zero external dependencies beyond openpyxl. Can be copied to any project.

Usage
-----
    from yumoyi_common.column_inference import FieldSpec, infer_columns, is_numeric, is_date_like

    fields = [
        FieldSpec("name", required=True, keywords=("姓名", "Name"), format_test=None),
        FieldSpec("amount", required=False, keywords=("金额", "Amount"), format_test=is_numeric),
    ]
    mapping = infer_columns(ws, fields)
    # mapping = {"name": 1, "amount": 3}  (1-based column indices)
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional


# ============================================================
# FieldSpec definition
# ============================================================

class FieldSpec:
    """
    Describes how to recognize a logical field in an Excel column.

    Parameters
    ----------
    name : logical field name (e.g. "code", "price")
    required : whether this field must be mapped for import to proceed
    keywords : header strings that suggest this field (matched case/space insensitive)
    format_test : function(cell_value) -> bool; returns True if a data cell
                  looks like this field. Called on sampled data rows, not headers.
    priority : higher = this field picks its column first when competing
    """

    def __init__(
        self,
        name: str,
        *,
        required: bool = False,
        keywords: tuple = (),
        format_test: Optional[Callable[[Any], bool]] = None,
        priority: int = 0,
    ):
        self.name = name
        self.required = required
        self.keywords = keywords
        self.format_test = format_test
        self.priority = priority


# ============================================================
# Scoring constants
# ============================================================

SCORE_FORMAT_MATCH = 100
SCORE_KEYWORD_EXACT = 80
SCORE_KEYWORD_CONTAINS = 50

SAMPLE_ROWS = 10
FORMAT_MATCH_THRESHOLD = 0.5

# Excel serial-number range treated as "date-like" by is_date_like().
# Override via the serial_range parameter when the default is too narrow
# or too wide for your data.
EXCEL_SERIAL_MIN = 30000   # ~1982-02
EXCEL_SERIAL_MAX = 60000   # ~2064-04


# ============================================================
# Internal helpers (self-contained, no external imports)
# ============================================================

def _cell_to_str(v: Any) -> str:
    """Convert any cell value to a clean string."""
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).decode("utf-8", errors="ignore").strip()
    if isinstance(v, float):
        if v == int(v):
            return str(int(v))
        return str(v).strip()
    return str(v).strip()


def _normalize_header(s: Any) -> str:
    """Lowercase, strip all whitespace for fuzzy header matching."""
    if s is None:
        return ""
    return re.sub(r"\s+", "", str(s).strip().lower())


# ============================================================
# Built-in format testers (generic, not business-specific)
# ============================================================

def is_numeric(val: Any) -> bool:
    """Value is a number (int or float)."""
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return True
    s = _cell_to_str(val)
    if not s:
        return False
    try:
        float(s.replace(",", ""))
        return True
    except ValueError:
        return False


def is_date_like(
    val: Any,
    *,
    serial_range: tuple = (EXCEL_SERIAL_MIN, EXCEL_SERIAL_MAX),
) -> bool:
    """Value looks like a date (datetime, date, or Excel serial number).

    Parameters
    ----------
    serial_range : (min, max) range for Excel serial-number detection.
                   Defaults to (EXCEL_SERIAL_MIN, EXCEL_SERIAL_MAX).
    """
    import datetime as dt
    if isinstance(val, (dt.datetime, dt.date)):
        return True
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        v = float(val)
        if serial_range[0] <= v <= serial_range[1]:
            return True
    s = _cell_to_str(val)
    if re.match(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}$", s):
        return True
    return False


# ============================================================
# Core inference logic
# ============================================================

def infer_columns(
    ws,
    field_specs: List[FieldSpec],
    header_row: int = 1,
    data_start_row: int = 2,
) -> Dict[str, Optional[int]]:
    """
    Infer column mapping for the given worksheet.

    Parameters
    ----------
    ws : openpyxl worksheet
    field_specs : list of FieldSpec describing the target fields
    header_row : which row contains headers (1-based)
    data_start_row : first data row (1-based)

    Returns
    -------
    {field_name: col_index (1-based)} for each field_spec.
    Unmapped fields have value None.
    """
    max_col = ws.max_column or 0
    if max_col == 0:
        return {fs.name: None for fs in field_specs}

    # Read headers
    headers = {}
    for c in range(1, max_col + 1):
        val = ws.cell(header_row, c).value
        headers[c] = _normalize_header(val) if val else ""

    # Sample data rows
    sample_end = min(data_start_row + SAMPLE_ROWS, (ws.max_row or 0) + 1)
    col_samples: Dict[int, List[Any]] = {c: [] for c in range(1, max_col + 1)}
    for r in range(data_start_row, sample_end):
        for c in range(1, max_col + 1):
            val = ws.cell(r, c).value
            if val is not None and str(val).strip():
                col_samples[c].append(val)

    # Score each (field, column) pair
    scores: Dict[str, Dict[int, int]] = {fs.name: {} for fs in field_specs}
    sorted_specs = sorted(field_specs, key=lambda fs: -fs.priority)

    for fs in sorted_specs:
        normalized_keywords = [_normalize_header(k) for k in fs.keywords]

        for c in range(1, max_col + 1):
            score = 0

            # 1. Format test on sampled data (highest priority)
            if fs.format_test and col_samples[c]:
                matches = sum(1 for v in col_samples[c] if fs.format_test(v))
                match_ratio = matches / len(col_samples[c])
                if match_ratio > FORMAT_MATCH_THRESHOLD:
                    score += int(SCORE_FORMAT_MATCH * match_ratio)

            # 2. Header keyword match
            hdr = headers[c]
            if hdr:
                for kw in normalized_keywords:
                    if hdr == kw:
                        score = max(score, SCORE_KEYWORD_EXACT)
                        break
                    if kw in hdr or hdr in kw:
                        score = max(score, SCORE_KEYWORD_CONTAINS)

            if score > 0:
                scores[fs.name][c] = score

    # Greedy assignment: highest-priority fields pick first, no column reuse
    mapping: Dict[str, Optional[int]] = {}
    used_cols: set = set()

    for fs in sorted_specs:
        best_col = None
        best_score = 0
        for c, s in scores[fs.name].items():
            if c not in used_cols and s > best_score:
                best_score = s
                best_col = c
        if best_col is not None:
            mapping[fs.name] = best_col
            used_cols.add(best_col)
        else:
            mapping[fs.name] = None

    return mapping
