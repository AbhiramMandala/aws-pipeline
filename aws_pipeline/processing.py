"""Glue-style ETL: clean raw CSVs in S3, write processed CSVs.

Same cleaning rules as scripts/data_utils (self-contained so this
package deploys alone): strip whitespace, lowercase emails, drop
rows missing id/name, drop id duplicates. Streams row-by-row.
"""
import csv
import io
import re
from typing import Dict, Iterable, Iterator, Optional, Tuple

from .config import PipelineConfig
from .storage import S3DataLake

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_NULL_TOKENS = {"", "null", "none", "n/a", "na", "nan", "nil", "-"}


def clean_record(row: Dict) -> Optional[Dict]:
    out = {}
    for k, v in row.items():
        v = v.strip() if isinstance(v, str) else v
        if isinstance(v, str) and v.lower() in _NULL_TOKENS:
            v = None
        out[k] = v
    if out.get("email") and not _EMAIL_RE.match(str(out["email"]).lower()):
        out["email"] = None
    if out.get("email"):
        out["email"] = str(out["email"]).lower()
    if not out.get("id") or not out.get("name"):
        return None
    try:
        out["id"] = str(int(float(str(out["id"]))))
    except (ValueError, TypeError):
        return None
    return out


def clean_rows(rows: Iterable[Dict]) -> Iterator[Dict]:
    seen = set()
    for row in rows:
        out = clean_record(row)
        if out is None or out["id"] in seen:
            continue
        seen.add(out["id"])
        yield out


def clean_csv_text(csv_text: str) -> Tuple[str, int, int]:
    """Clean one CSV document. Returns (cleaned_csv, rows_in, rows_out)."""
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)  # single pass so id-dedupe sees all rows
    cleaned = list(clean_rows(rows))
    rows_in, rows_out = len(rows), len(cleaned)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=reader.fieldnames or ["id", "name", "email", "city"])
    writer.writeheader()
    for row in cleaned:
        writer.writerow({k: ("" if v is None else v) for k, v in row.items()})
    return buf.getvalue(), rows_in, len(cleaned)


def run_etl(lake: S3DataLake, src_prefix: Optional[str] = None,
            dst_prefix: Optional[str] = None) -> Dict:
    """Process every raw CSV into processed/. Returns counts."""
    src_prefix = src_prefix or lake.cfg.raw_prefix
    dst_prefix = dst_prefix or lake.cfg.processed_prefix
    files, rows_in, rows_out = 0, 0, 0
    for key in lake.list_keys(src_prefix):
        if not key.lower().endswith(".csv"):
            continue
        cleaned, n_in, n_out = clean_csv_text(lake.read_text(key))
        name = key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        lake.write_text(f"{dst_prefix}{name}.clean.csv", cleaned)
        files += 1
        rows_in += n_in
        rows_out += n_out
    return {"files": files, "rows_in": rows_in, "rows_out": rows_out}
