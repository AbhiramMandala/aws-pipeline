"""End-to-end pipeline: storage -> catalog -> ETL -> analytics -> IAM."""
from typing import Any, Dict, Optional

import boto3

from .analytics import AthenaRunner
from .catalog import GlueCatalog
from .config import PipelineConfig
from .iam import ensure_pipeline_roles
from .processing import run_etl
from .storage import S3DataLake

SAMPLE_RAW = (
    "id,name,email,city\n"
    "1, Ada Lovelace ,ADA@EXAMPLE.COM,London\n"
    "2,Alan Turing,alan@example.com,  Manchester \n"
    "3,,N/A,Leeds\n"
    "2,Alan Turing,alan@example.com,Manchester\n"
)


def run_pipeline(cfg: Optional[PipelineConfig] = None,
                 clients: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Build the lake, catalog it, clean it, query it. Returns a summary."""
    cfg = cfg or PipelineConfig()
    clients = clients or {}
    region_kw = {"region_name": cfg.region}

    lake = S3DataLake(cfg, clients.get("s3") or boto3.client("s3", **region_kw))
    lake.ensure_bucket()
    raw_key = lake.put_raw("users_001.csv", SAMPLE_RAW.encode("utf-8"))

    iam_client = clients.get("iam") or boto3.client("iam", **region_kw)
    roles = ensure_pipeline_roles(cfg, iam_client)

    catalog = GlueCatalog(cfg, clients.get("glue") or boto3.client("glue", **region_kw))
    catalog.ensure_table()
    crawler = catalog.ensure_crawler(roles["glue_role"])
    location = catalog.table_location()

    etl = run_etl(lake)

    athena = AthenaRunner(cfg, clients.get("athena") or boto3.client("athena", **region_kw))
    stats = athena.stats(lake)

    return {
        "bucket": cfg.bucket,
        "raw_key": raw_key,
        "database": cfg.glue_db,
        "table": cfg.table,
        "table_location": location,
        "crawler": crawler,
        "etl": etl,
        "analytics": stats,
        "roles": roles,
    }
