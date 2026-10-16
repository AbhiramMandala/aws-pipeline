"""Lambda: S3 ObjectCreated trigger -> validate + clean -> staging/.

Deploy: zip this package and set the handler to
``lambda_handler.lambda_handler`` with an S3 trigger on ``raw/``.
Returns a summary dict (also logged for CloudWatch).
"""
from typing import Any, Dict, List, Optional

from .config import PipelineConfig
from .processing import clean_csv_text
from .storage import S3DataLake


def _record_to_key(record: Dict) -> Optional[str]:
    try:
        return record["s3"]["object"]["key"]
    except KeyError:
        return None


def lambda_handler(event: Dict, context: Any = None,
                   cfg: Optional[PipelineConfig] = None,
                   lake: Optional[S3DataLake] = None) -> Dict:
    cfg = cfg or PipelineConfig(
        bucket=os_bucket(),
        region=os_region(),
    )
    lake = lake or S3DataLake(cfg)
    outputs: List[str] = []
    skipped: List[str] = []
    errors: Dict[str, str] = {}
    rows_in = rows_out = 0
    for record in event.get("Records", []):
        key = _record_to_key(record)
        if not key or not key.lower().endswith(".csv"):
            skipped.append(key or "<no-key>")
            continue
        try:
            cleaned, n_in, n_out = clean_csv_text(lake.read_text(key))
        except Exception as exc:  # one bad file must not fail the batch
            print(f"lambda_handler: skipping unreadable file {key}: {exc}")
            skipped.append(key)
            errors[key] = f"{type(exc).__name__}: {exc}"
            continue
        name = key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        out_key = f"{cfg.staging_prefix}{name}.clean.csv"
        lake.write_text(out_key, cleaned)
        outputs.append(out_key)
        rows_in += n_in
        rows_out += n_out
    print(f"lambda_handler: processed={len(outputs)} skipped={len(skipped)} "
          f"rows_in={rows_in} rows_out={rows_out}")
    return {"processed": len(outputs), "outputs": outputs,
            "skipped": skipped, "errors": errors,
            "rows_in": rows_in, "rows_out": rows_out}


def os_bucket() -> str:
    import os

    return os.environ.get("LAKE_BUCKET", PipelineConfig.bucket)


def os_region() -> str:
    import os

    return os.environ.get("AWS_REGION", PipelineConfig.region)
