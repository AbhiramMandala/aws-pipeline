"""S3 secure storage: bucket with versioning + default encryption (SSE-S3).

Landing zones are prefixes: raw/ (lands first), staging/ (cleaning),
processed/ (analytics-ready). All writes use SSE-AES256.
"""
from typing import Iterator, Optional

import boto3

from .config import PipelineConfig


class S3DataLake:
    def __init__(self, cfg: Optional[PipelineConfig] = None, s3=None):
        self.cfg = cfg or PipelineConfig()
        self.s3 = s3 or boto3.client("s3", region_name=self.cfg.region)

    # -- setup ---------------------------------------------------------
    def ensure_bucket(self) -> str:
        """Create bucket (if missing) + versioning + AES256 default encryption."""
        try:
            if self.cfg.region == "us-east-1":
                self.s3.create_bucket(Bucket=self.cfg.bucket)
            else:
                self.s3.create_bucket(
                    Bucket=self.cfg.bucket,
                    CreateBucketConfiguration={"LocationConstraint": self.cfg.region},
                )
        except self.s3.exceptions.BucketAlreadyOwnedByYou:
            pass
        self.s3.put_bucket_versioning(
            Bucket=self.cfg.bucket, VersioningConfiguration={"Status": "Enabled"}
        )
        self.s3.put_bucket_encryption(
            Bucket=self.cfg.bucket,
            ServerSideEncryptionConfiguration={
                "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
            },
        )
        for prefix in (self.cfg.raw_prefix, self.cfg.staging_prefix,
                       self.cfg.processed_prefix, self.cfg.athena_output_prefix):
            self.s3.put_object(Bucket=self.cfg.bucket, Key=prefix, Body=b"")
        return self.cfg.bucket

    # -- I/O ------------------------------------------------------------
    def put_raw(self, name: str, body: bytes) -> str:
        key = self.cfg.raw_prefix + name
        self.s3.put_object(Bucket=self.cfg.bucket, Key=key, Body=body,
                           ServerSideEncryption="AES256")
        return key

    def write_text(self, key: str, text: str) -> str:
        self.s3.put_object(Bucket=self.cfg.bucket, Key=key,
                           Body=text.encode("utf-8"), ServerSideEncryption="AES256")
        return key

    def read_text(self, key: str) -> str:
        obj = self.s3.get_object(Bucket=self.cfg.bucket, Key=key)
        return obj["Body"].read().decode("utf-8")

    def copy_key(self, src_key: str, dst_key: str) -> str:
        self.s3.copy_object(Bucket=self.cfg.bucket, Key=dst_key,
                            CopySource={"Bucket": self.cfg.bucket, "Key": src_key})
        return dst_key

    # -- streaming -------------------------------------------------------
    def list_keys(self, prefix: str) -> Iterator[str]:
        """Yield keys under a prefix (paginated, constant memory)."""
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.cfg.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith("/"):  # skip folder markers
                    yield obj["Key"]

    def count(self, prefix: str) -> int:
        return sum(1 for _ in self.list_keys(prefix))
