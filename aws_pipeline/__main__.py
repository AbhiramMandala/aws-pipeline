"""CLI: run the whole pipeline against mocked AWS (moto).

Usage:
    python -m aws_pipeline
    python -m aws_pipeline --bucket my-test-lake --region us-east-1
"""
import argparse
import json

from moto import mock_aws

from .config import PipelineConfig
from .pipeline import run_pipeline


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="AWS data pipeline (mocked demo)")
    ap.add_argument("--bucket", default=PipelineConfig.bucket)
    ap.add_argument("--region", default=PipelineConfig.region)
    args = ap.parse_args(argv)
    cfg = PipelineConfig(bucket=args.bucket, region=args.region)
    with mock_aws():
        summary = run_pipeline(cfg)
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
