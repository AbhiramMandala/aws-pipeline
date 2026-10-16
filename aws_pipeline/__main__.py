"""CLI: run the whole pipeline against mocked AWS (moto).

Usage:
    python -m aws_pipeline
    python -m aws_pipeline --bucket my-test-lake --region us-east-1
    python -m aws_pipeline --input-file data/users_002.csv
"""
import argparse
import json
from pathlib import Path

from moto import mock_aws

from .config import PipelineConfig
from .pipeline import run_pipeline


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="AWS data pipeline (mocked demo)")
    ap.add_argument("--bucket", default=PipelineConfig.bucket)
    ap.add_argument("--region", default=PipelineConfig.region)
    ap.add_argument("--input-file", default=None,
                    help="Local CSV to upload as the raw dataset instead of the bundled sample")
    args = ap.parse_args(argv)
    cfg = PipelineConfig(bucket=args.bucket, region=args.region)
    raw_name, raw_payload = "users_001.csv", None
    if args.input_file:
        raw_name = Path(args.input_file).name
        raw_payload = Path(args.input_file).read_bytes()
    with mock_aws():
        summary = run_pipeline(cfg, raw_payload=raw_payload, raw_name=raw_name)
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
