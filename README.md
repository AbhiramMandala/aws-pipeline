# AWS Data Pipeline (S3 + Glue + Athena + Lambda + IAM) 

Standalone copy. All AWS calls run against moto mocks, no account needed.

## Setup (once)

```
pip install boto3 moto pyparsing
```

## Run (from this folder)

```
python -m unittest discover -s tests -v
python -m aws_pipeline --bucket demo-lake-bucket
```

## Deploy for real later

Configure AWS credentials, then in `aws_pipeline/__main__.py`
replace the `mock_aws()` wrapper with a direct `run_pipeline()` call.
Zip `aws_pipeline/` for Lambda with handler `lambda_handler.lambda_handler`.
