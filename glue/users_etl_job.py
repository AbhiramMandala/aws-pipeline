"""AWS Glue Python Shell job: clean raw CSVs into processed/.

Runs the exact same rules as aws_pipeline.processing (imported from the
deployment zip via --extra-py-files). Reads its settings from Glue job
arguments when running on Glue, or from plain CLI flags locally::

    # Local dry-run (no AWS):
    python glue/users_etl_job.py --input-prefix raw/users/ --output-prefix processed/users/

    # On Glue (Python Shell): pass --extra-py-files s3://<bucket>/lib/aws_pipeline.zip
    # and job arguments --input-prefix / --output-prefix / --region / --bucket.

Idempotent: output keys are deterministic per input file, so reruns overwrite
the same objects instead of duplicating data.
"""
import argparse
import sys
from pathlib import Path

# Allow running from the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from aws_pipeline.config import PipelineConfig  # noqa: E402
from aws_pipeline.processing import run_etl  # noqa: E402
from aws_pipeline.storage import S3DataLake  # noqa: E402


def parse_args(argv=None):
    """Glue getResolvedOptions when available, argparse otherwise."""
    try:
        from awsglue.utils import getResolvedOptions  # type: ignore

        args = getResolvedOptions(
            argv or sys.argv,
            ["JOB_NAME", "input-prefix", "output-prefix", "region", "bucket"],
        )
        ns = argparse.Namespace(
            input_prefix=args.get("input-prefix") or PipelineConfig.raw_prefix,
            output_prefix=args.get("output-prefix") or PipelineConfig.processed_prefix,
            region=args.get("region") or PipelineConfig.region,
            bucket=args.get("bucket") or PipelineConfig.bucket,
        )
        return ns
    except ImportError:
        ap = argparse.ArgumentParser(description="Users ETL Glue job (Python Shell)")
        ap.add_argument("--input-prefix", default=PipelineConfig.raw_prefix)
        ap.add_argument("--output-prefix", default=PipelineConfig.processed_prefix)
        ap.add_argument("--region", default=PipelineConfig.region)
        ap.add_argument("--bucket", default=PipelineConfig.bucket)
        return ap.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    cfg = PipelineConfig(
        bucket=args.bucket,
        region=args.region,
        raw_prefix=args.input_prefix,
        processed_prefix=args.output_prefix,
    )
    lake = S3DataLake(cfg)
    lake.ensure_bucket()
    result = run_etl(lake)
    print(f"users_etl_job: {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
