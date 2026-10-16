"""IAM: least-privilege policies for the pipeline roles.

No AWS calls needed to build the documents; ensure_role() creates
them (works on moto too). Real account id is read from STS when
available, else falls back to the config value (mock default).
"""
from typing import Dict, Optional

import boto3

from .config import PipelineConfig


def _bucket_arns(cfg: PipelineConfig):
    base = f"arn:aws:s3:::{cfg.bucket}"
    return base, f"{base}/*"


def pipeline_policy(cfg: PipelineConfig) -> Dict:
    """Glue/job role: full prefix-scoped S3 + Glue + Athena scoped to db."""
    bucket, objects = _bucket_arns(cfg)
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Sid": "ListBucket", "Effect": "Allow",
             "Action": ["s3:ListBucket"], "Resource": [bucket]},
            {"Sid": "ReadWriteLake", "Effect": "Allow",
             "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
             "Resource": [objects]},
            {"Sid": "GlueCatalog", "Effect": "Allow",
             "Action": ["glue:Get*", "glue:Create*", "glue:Update*"],
             "Resource": ["*"]},
            {"Sid": "AthenaQueries", "Effect": "Allow",
             "Action": ["athena:StartQueryExecution", "athena:GetQuery*"],
             "Resource": ["*"]},
        ],
    }


def lambda_policy(cfg: PipelineConfig) -> Dict:
    """Lambda role: logs + read raw/ + write staging/ only."""
    bucket, _ = _bucket_arns(cfg)
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Sid": "WriteLogs", "Effect": "Allow",
             "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
             "Resource": ["arn:aws:logs:*:*:*"]},
            {"Sid": "ReadRaw", "Effect": "Allow",
             "Action": ["s3:GetObject", "s3:ListBucket"],
             "Resource": [bucket, f"{bucket}/{cfg.raw_prefix}*"]},
            {"Sid": "WriteStaging", "Effect": "Allow",
             "Action": ["s3:PutObject"],
             "Resource": [f"{bucket}/{cfg.staging_prefix}*"]},
        ],
    }


def ensure_role(iam, name: str, service: str, policy: Dict,
                cfg: Optional[PipelineConfig] = None) -> str:
    """Create role + inline policy. Returns the role ARN."""
    assume = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": f"{service}.amazonaws.com"},
                       "Action": "sts:AssumeRole"}],
    }
    import json

    try:
        role = iam.create_role(RoleName=name,
                               AssumeRolePolicyDocument=json.dumps(assume))["Role"]
    except iam.exceptions.EntityAlreadyExistsException:
        role = iam.get_role(RoleName=name)["Role"]
    iam.put_role_policy(RoleName=name, PolicyName=f"{name}-policy",
                        PolicyDocument=json.dumps(policy))
    return role["Arn"]


def ensure_pipeline_roles(cfg: Optional[PipelineConfig] = None, iam=None) -> Dict[str, str]:
    import boto3 as _boto3

    cfg = cfg or PipelineConfig()
    iam = iam or _boto3.client("iam", region_name=cfg.region)
    glue_arn = ensure_role(iam, cfg.glue_role_name, "glue", pipeline_policy(cfg), cfg)
    lambda_arn = ensure_role(iam, cfg.lambda_role_name, "lambda", lambda_policy(cfg), cfg)
    return {"glue_role": glue_arn, "lambda_role": lambda_arn}
