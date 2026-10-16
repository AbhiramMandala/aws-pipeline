"""Cloud data pipeline on AWS (S3 + Glue + Athena + Lambda + IAM).

Runs for real against AWS with credentials, or fully mocked with moto::

    python -m aws_pipeline --bucket my-lake-dev

Layout: config -> storage (S3) -> catalog (Glue) -> processing (ETL) ->
analytics (Athena) -> lambda_handler (S3 trigger) -> iam (policies).
"""
from .config import PipelineConfig

__all__ = ["PipelineConfig"]
