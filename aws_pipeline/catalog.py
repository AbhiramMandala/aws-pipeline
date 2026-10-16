"""Glue Data Catalog: database + table + crawler over the lake."""
from typing import List, Optional

import boto3

from .config import PipelineConfig


class GlueCatalog:
    def __init__(self, cfg: Optional[PipelineConfig] = None, glue=None):
        self.cfg = cfg or PipelineConfig()
        self.glue = glue or boto3.client("glue", region_name=self.cfg.region)

    def ensure_database(self) -> str:
        try:
            self.glue.create_database(DatabaseInput={"Name": self.cfg.glue_db})
        except self.glue.exceptions.AlreadyExistsException:
            pass
        return self.cfg.glue_db

    def ensure_table(self, columns: Optional[List[str]] = None) -> str:
        columns = columns or self.cfg.columns
        self.ensure_database()
        cols = [{"Name": c, "Type": "string"} for c in columns]
        try:
            self.glue.create_table(
                DatabaseName=self.cfg.glue_db,
                TableInput={
                    "Name": self.cfg.table,
                    "StorageDescriptor": {
                        "Columns": cols,
                        "Location": self.cfg.processed_location,
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                        "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                                      "Parameters": {"skip.header.line.count": "1"}},
                    },
                    "PartitionKeys": [],
                    "TableType": "EXTERNAL_TABLE",
                    "Parameters": {"classification": "csv"},
                },
            )
        except self.glue.exceptions.AlreadyExistsException:
            pass
        return self.cfg.table

    def ensure_crawler(self, role_arn: str) -> str:
        """Crawler scans raw/ and registers new partitions on a schedule."""
        try:
            self.glue.create_crawler(
                Name=self.cfg.crawler,
                Role=role_arn,
                DatabaseName=self.cfg.glue_db,
                Targets={"S3Targets": [{"Path": f"s3://{self.cfg.bucket}/{self.cfg.raw_prefix}"}]},
                Schedule="cron(0 1 * * ? *)",  # nightly 01:00 UTC
            )
        except self.glue.exceptions.AlreadyExistsException:
            pass
        return self.glue.get_crawler(Name=self.cfg.crawler)["Crawler"]["Name"]

    def table_location(self) -> str:
        table = self.glue.get_table(DatabaseName=self.cfg.glue_db, Name=self.cfg.table)["Table"]
        return table["StorageDescriptor"]["Location"]
