"""Athena analytics: SQL builders + submit/wait/fetch.

Under moto the query SUCCEEDs but returns no rows, so stats() also
computes the same aggregates directly from the processed CSVs in S3 —
the submitted SQL is what you would run for real in the console.
"""
import csv
import io
import time
from collections import Counter
from typing import Any, Dict, Iterator, List, Optional

import boto3

from .config import PipelineConfig
from .storage import S3DataLake


class AthenaRunner:
    def __init__(self, cfg: Optional[PipelineConfig] = None, athena=None):
        self.cfg = cfg or PipelineConfig()
        self.athena = athena or boto3.client("athena", region_name=self.cfg.region)

    # -- SQL builders ----------------------------------------------------
    def q_count(self) -> str:
        return f"SELECT COUNT(*) AS n FROM {self.cfg.glue_db}.{self.cfg.table}"

    def q_by_city(self) -> str:
        return (
            f"SELECT city, COUNT(*) AS n FROM {self.cfg.glue_db}.{self.cfg.table} "
            "GROUP BY city ORDER BY n DESC LIMIT 10"
        )

    # -- execution --------------------------------------------------------
    def start(self, sql: str) -> str:
        resp = self.athena.start_query_execution(
            QueryString=sql,
            ResultConfiguration={"OutputLocation": self.cfg.athena_output},
        )
        return resp["QueryExecutionId"]

    def wait(self, execution_id: str, timeout: int = 60) -> str:
        deadline = time.time() + timeout
        while time.time() < deadline:
            state = self.athena.get_query_execution(
                QueryExecutionId=execution_id)["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                return state
            time.sleep(1)
        return "TIMEOUT"

    def fetch_rows(self, execution_id: str) -> Iterator[Dict]:
        paginator = self.athena.get_paginator("get_query_results")
        header: List[str] = []
        for i, page in enumerate(paginator.paginate(QueryExecutionId=execution_id)):
            rows = page["ResultSet"]["Rows"]
            if i == 0:
                header = [c.get("VarCharValue", "") for c in rows[0]["Data"]]
                rows = rows[1:]
            for r in rows:
                vals = [c.get("VarCharValue", "") for c in r["Data"]]
                yield dict(zip(header, vals))

    def run(self, sql: str) -> List[Dict]:
        execution_id = self.start(sql)
        state = self.wait(execution_id)
        if state != "SUCCEEDED":
            raise RuntimeError(f"Athena query {state}: {sql}")
        return list(self.fetch_rows(execution_id))

    # -- stats over processed data (works identically on mock or real S3)
    def stats(self, lake: S3DataLake) -> Dict[str, Any]:
        """Submit the analytics SQL to Athena, compute aggregates from S3."""
        execution_id = self.start(self.q_by_city())
        state = self.wait(execution_id)
        total, cities = 0, Counter()
        for key in lake.list_keys(self.cfg.processed_prefix):
            if not key.lower().endswith(".csv"):
                continue
            for row in csv.DictReader(io.StringIO(lake.read_text(key))):
                total += 1
                if row.get("city"):
                    cities[row["city"]] += 1
        return {"athena_state": state, "total_rows": total,
                "by_city": dict(cities.most_common(10))}
