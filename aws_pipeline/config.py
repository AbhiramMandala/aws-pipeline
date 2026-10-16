"""Shared pipeline settings (one place to change names/region)."""
from dataclasses import dataclass, field
from typing import List


@dataclass
class PipelineConfig:
    region: str = "us-east-1"
    bucket: str = "company-data-lake-dev"
    account_id: str = "123456789012"  # mock default; real id comes from STS
    raw_prefix: str = "raw/users/"
    staging_prefix: str = "staging/users/"
    processed_prefix: str = "processed/users/"
    athena_output_prefix: str = "athena-results/"
    glue_db: str = "analytics_db"
    table: str = "users"
    crawler: str = "users-crawler"
    lambda_role_name: str = "data-pipeline-lambda-role"
    glue_role_name: str = "data-pipeline-glue-role"
    columns: List[str] = field(default_factory=lambda: ["id", "name", "email", "city"])

    @property
    def athena_output(self) -> str:
        return f"s3://{self.bucket}/{self.athena_output_prefix}"

    @property
    def processed_location(self) -> str:
        return f"s3://{self.bucket}/{self.processed_prefix}"
