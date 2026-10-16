"""Tests for aws_pipeline (all AWS calls mocked with moto)."""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import boto3
from moto import mock_aws

from aws_pipeline import PipelineConfig
from aws_pipeline.analytics import AthenaRunner
from aws_pipeline.catalog import GlueCatalog
from aws_pipeline.iam import ensure_pipeline_roles, lambda_policy, pipeline_policy
from aws_pipeline.lambda_handler import lambda_handler
from aws_pipeline.pipeline import SAMPLE_RAW, run_pipeline
from aws_pipeline.processing import clean_csv_text, clean_record, run_etl
from aws_pipeline.storage import S3DataLake


def make_cfg(**over):
    params = {"bucket": "test-lake-bucket", "region": "us-east-1"}
    params.update(over)
    return PipelineConfig(**params)


class StorageTest(unittest.TestCase):
    def setUp(self):
        self.mock = mock_aws()
        self.mock.start()
        self.cfg = make_cfg()

    def tearDown(self):
        self.mock.stop()

    def test_bucket_zones_and_encrypted_upload(self):
        lake = S3DataLake(self.cfg)
        lake.ensure_bucket()
        s3 = boto3.client("s3", region_name="us-east-1")
        versioning = s3.get_bucket_versioning(Bucket=self.cfg.bucket)
        self.assertEqual(versioning["Status"], "Enabled")
        enc = s3.get_bucket_encryption(Bucket=self.cfg.bucket)
        algos = [r["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
                 for r in enc["ServerSideEncryptionConfiguration"]["Rules"]]
        self.assertIn("AES256", algos)
        key = lake.put_raw("users_001.csv", b"id,name\n1,Ada\n")
        obj = s3.head_object(Bucket=self.cfg.bucket, Key=key)
        self.assertEqual(obj["ServerSideEncryption"], "AES256")
        self.assertEqual(lake.read_text(key), "id,name\n1,Ada\n")
        self.assertEqual(list(lake.list_keys(self.cfg.raw_prefix)), [key])


class ProcessingTest(unittest.TestCase):
    def test_clean_record_rules(self):
        self.assertEqual(
            clean_record({"id": " 1 ", "name": " Ada ", "email": "ADA@X.COM", "city": " N/A "}),
            {"id": "1", "name": "Ada", "email": "ada@x.com", "city": None},
        )
        self.assertIsNone(clean_record({"id": "", "name": "", "email": "", "city": ""}))
        self.assertIsNone(clean_record({"id": "abc", "name": "No"}))

    def test_clean_csv_text(self):
        cleaned, n_in, n_out = clean_csv_text(SAMPLE_RAW)
        self.assertEqual(n_in, 4)
        self.assertEqual(n_out, 2)  # empty-name + dup id dropped
        self.assertIn("ada@example.com", cleaned)

    def test_run_etl_end_to_end(self):
        with mock_aws():
            cfg = make_cfg()
            lake = S3DataLake(cfg)
            lake.ensure_bucket()
            lake.put_raw("users_001.csv", SAMPLE_RAW.encode())
            lake.put_raw("notes.txt", b"ignore me")
            res = run_etl(lake)
            self.assertEqual(res, {"files": 1, "rows_in": 4, "rows_out": 2})
            out = list(lake.list_keys(cfg.processed_prefix))
            self.assertEqual(len(out), 1)
            self.assertIn("ada@example.com", lake.read_text(out[0]))


class CatalogTest(unittest.TestCase):
    def test_database_table_crawler(self):
        with mock_aws():
            cfg = make_cfg()
            cat = GlueCatalog(cfg)
            cat.ensure_table()
            self.assertEqual(cat.table_location(), cfg.processed_location)
            crawler = cat.ensure_crawler("arn:aws:iam::123456789012:role/r")
            self.assertEqual(crawler, cfg.crawler)


class AnalyticsTest(unittest.TestCase):
    def test_sql_builders_and_stats(self):
        with mock_aws():
            cfg = make_cfg()
            lake = S3DataLake(cfg)
            lake.ensure_bucket()
            lake.write_text(cfg.processed_prefix + "a.clean.csv",
                            "id,name,email,city\n1,Ada,a@x.com,London\n2,Al,b@x.com,London\n")
            runner = AthenaRunner(cfg)
            self.assertIn("analytics_db.users", runner.q_by_city())
            stats = runner.stats(lake)
            self.assertEqual(stats["athena_state"], "SUCCEEDED")
            self.assertEqual(stats["total_rows"], 2)
            self.assertEqual(stats["by_city"], {"London": 2})


class LambdaTest(unittest.TestCase):
    def test_s3_trigger_cleans_to_staging(self):
        with mock_aws():
            cfg = make_cfg()
            lake = S3DataLake(cfg)
            lake.ensure_bucket()
            key = lake.put_raw("users_001.csv", SAMPLE_RAW.encode())
            event = {"Records": [{"s3": {"bucket": {"name": cfg.bucket},
                                         "object": {"key": key}}},
                                 {"s3": {"bucket": {"name": cfg.bucket},
                                         "object": {"key": "raw/users/pic.png"}}}]}
            res = lambda_handler(event, None, cfg=cfg, lake=lake)
            self.assertEqual(res["processed"], 1)
            self.assertEqual(res["skipped"], ["raw/users/pic.png"])
            self.assertEqual(res["rows_out"], 2)
            self.assertEqual(lake.count(cfg.staging_prefix), 1)


class IamTest(unittest.TestCase):
    def test_policy_documents_least_privilege(self):
        cfg = make_cfg()
        pol = pipeline_policy(cfg)
        sids = {s["Sid"] for s in pol["Statement"]}
        self.assertTrue({"ListBucket", "ReadWriteLake", "GlueCatalog", "AthenaQueries"} <= sids)
        lam = lambda_policy(cfg)
        actions = [a for s in lam["Statement"] for a in s["Action"]]
        self.assertIn("logs:PutLogEvents", actions)
        self.assertNotIn("s3:DeleteObject", actions)  # lambda must not delete

    def test_roles_created(self):
        with mock_aws():
            cfg = make_cfg()
            iam = boto3.client("iam", region_name="us-east-1")
            roles = ensure_pipeline_roles(cfg, iam)
            self.assertIn("role", roles["glue_role"])
            stored = iam.get_role_policy(RoleName=cfg.lambda_role_name,
                                         PolicyName=f"{cfg.lambda_role_name}-policy")
            doc = json.loads(stored["PolicyDocument"]) \
                if isinstance(stored["PolicyDocument"], str) else stored["PolicyDocument"]
            self.assertIn("Statement", doc)


class PipelineTest(unittest.TestCase):
    def test_full_pipeline(self):
        with mock_aws():
            summary = run_pipeline(make_cfg())
            self.assertEqual(summary["bucket"], "test-lake-bucket")
            self.assertEqual(summary["etl"]["rows_out"], 2)
            self.assertEqual(summary["analytics"]["total_rows"], 2)
            self.assertEqual(summary["analytics"]["athena_state"], "SUCCEEDED")
            self.assertEqual(summary["crawler"], make_cfg().crawler)


class SecurityConfigTest(unittest.TestCase):
    def test_public_access_blocked(self):
        with mock_aws():
            cfg = make_cfg()
            S3DataLake(cfg).ensure_bucket()
            s3 = boto3.client("s3", region_name="us-east-1")
            pab = s3.get_public_access_block(Bucket=cfg.bucket)["PublicAccessBlockConfiguration"]
            self.assertTrue(all(pab[k] for k in ("BlockPublicAcls", "IgnorePublicAcls",
                                                 "BlockPublicPolicy", "RestrictPublicBuckets")))

    def test_config_env_overrides(self):
        import os

        from aws_pipeline.lambda_handler import os_bucket, os_region
        old_b, old_r = os.environ.get("LAKE_BUCKET"), os.environ.get("AWS_REGION")
        try:
            os.environ["LAKE_BUCKET"] = "env-bucket"
            os.environ["AWS_REGION"] = "eu-west-1"
            self.assertEqual(os_bucket(), "env-bucket")
            self.assertEqual(os_region(), "eu-west-1")
        finally:
            if old_b is None:
                os.environ.pop("LAKE_BUCKET", None)
            else:
                os.environ["LAKE_BUCKET"] = old_b
            if old_r is None:
                os.environ.pop("AWS_REGION", None)
            else:
                os.environ["AWS_REGION"] = old_r
        self.assertEqual(os_bucket(), PipelineConfig.bucket)


class EdgeCaseTest(unittest.TestCase):
    def test_empty_file(self):
        cleaned, n_in, n_out = clean_csv_text("id,name,email,city\n")
        self.assertEqual((n_in, n_out), (0, 0))
        self.assertIn("id,name,email,city", cleaned)

    def test_missing_columns(self):
        cleaned, n_in, n_out = clean_csv_text("id,city\n1,London\n")
        self.assertEqual((n_in, n_out), (1, 0))  # no name column -> dropped

    def test_malformed_ids_and_emails(self):
        text = ("id,name,email,city\n"
                "abc,No,ok@x.com,London\n"
                "7,Bad Email,not-an-email,Leeds\n"
                "8,Null Mail,NULL,York\n"
                "9,Ok,ok@x.com,Bath\n")
        cleaned, n_in, n_out = clean_csv_text(text)
        self.assertEqual((n_in, n_out), (4, 3))
        self.assertIn("ok@x.com", cleaned)
        self.assertNotIn("not-an-email", cleaned)

    def test_lambda_skips_unreadable_file_but_processes_rest(self):
        import aws_pipeline.lambda_handler as lh

        with mock_aws():
            cfg = make_cfg()
            lake = S3DataLake(cfg)
            lake.ensure_bucket()
            good = lake.put_raw("users_001.csv", SAMPLE_RAW.encode())
            # CSV key whose body is not decodable UTF-8.
            bad = lake.put_raw("users_002.csv", b"\xff\xfe\x00bad")
            event = {"Records": [{"s3": {"object": {"key": good}}},
                                 {"s3": {"object": {"key": bad}}}]}
            res = lh.lambda_handler(event, None, cfg=cfg, lake=lake)
            self.assertEqual(res["processed"], 1)
            self.assertIn(bad, res["skipped"])
            self.assertIn(bad, res["errors"])
            self.assertEqual(res["rows_out"], 2)


class SampleDataTest(unittest.TestCase):
    def test_bundled_csvs_clean_to_expected_counts(self):
        from pathlib import Path

        data = Path(__file__).resolve().parents[1] / "data"
        self.assertTrue((data / "users_001.csv").exists())
        self.assertTrue((data / "users_002.csv").exists())
        _, _, out1 = clean_csv_text((data / "users_001.csv").read_text())
        _, _, out2 = clean_csv_text((data / "users_002.csv").read_text())
        self.assertEqual(out1, 2)
        self.assertEqual(out2, 5)


class CliTest(unittest.TestCase):
    def test_input_file_replaces_sample(self):
        import tempfile

        from aws_pipeline.__main__ import main

        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "custom.csv"
            p.write_text("id,name,email,city\n10,Zoe,zoe@x.com,Oslo\n")
            with mock_aws():
                # main() prints the summary; capture via redirect
                import io
                from contextlib import redirect_stdout
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = main(["--bucket", "cli-bucket", "--input-file", str(p)])
                self.assertEqual(rc, 0)
                summary = json.loads(buf.getvalue())
        self.assertEqual(summary["etl"], {"files": 1, "rows_in": 1, "rows_out": 1})


class AnalyticsExtTest(unittest.TestCase):
    def test_email_domain_builder_and_stats(self):
        from aws_pipeline.analytics import AthenaRunner

        with mock_aws():
            cfg = make_cfg()
            runner = AthenaRunner(cfg)
            self.assertIn("analytics_db.users", runner.q_email_domains())
            lake = S3DataLake(cfg)
            lake.ensure_bucket()
            lake.write_text(cfg.processed_prefix + "a.clean.csv",
                            "id,name,email,city\n1,Ada,a@x.com,London\n2,Al,b@x.com,Leeds\n")
            stats = runner.stats(lake)
            self.assertEqual(stats["by_domain"], {"x.com": 2})


class GlueJobTest(unittest.TestCase):
    def test_arg_parsing_defaults(self):
        from glue.users_etl_job import parse_args

        args = parse_args([])
        self.assertEqual(args.input_prefix, "raw/users/")
        self.assertEqual(args.output_prefix, "processed/users/")

    def test_job_runs_against_moto(self):
        from glue.users_etl_job import main as job_main

        with mock_aws():
            cfg = make_cfg()
            lake = S3DataLake(cfg)
            lake.ensure_bucket()
            lake.put_raw("users_001.csv", SAMPLE_RAW.encode())
            rc = job_main(["--bucket", cfg.bucket, "--region", cfg.region,
                           "--input-prefix", cfg.raw_prefix,
                           "--output-prefix", cfg.processed_prefix])
            self.assertEqual(rc, 0)
            self.assertEqual(lake.count(cfg.processed_prefix), 1)


class InfraFilesTest(unittest.TestCase):
    def test_cloudformation_template_valid(self):
        import yaml

        def cfn_constructor(loader, tag_suffix, node):
            if isinstance(node, yaml.ScalarNode):
                return {tag_suffix: loader.construct_scalar(node)}
            if isinstance(node, yaml.SequenceNode):
                return {tag_suffix: loader.construct_sequence(node)}
            return {tag_suffix: loader.construct_mapping(node)}

        yaml.SafeLoader.add_multi_constructor("!", cfn_constructor)
        root = Path(__file__).resolve().parents[1]
        with open(root / "infrastructure" / "cloudformation.yaml") as fh:
            tpl = yaml.safe_load(fh)
        self.assertIn("Resources", tpl)
        self.assertIn("Parameters", tpl)
        resources = tpl["Resources"]
        for name in ("LakeBucket", "GlueRole", "LambdaRole", "CleaningFunction",
                     "GlueDatabase", "UsersTable", "UsersCrawler", "AnalyticsWorkgroup"):
            self.assertIn(name, resources)
        text = json.dumps(tpl)
        self.assertNotIn("123456789012", text)  # no hardcoded account ids
        for secret_word in ("AKIA", "aws_secret", "password"):
            self.assertNotIn(secret_word, text.lower())

    def test_sql_files_present_and_sane(self):
        root = Path(__file__).resolve().parents[1] / "sql"
        files = sorted(root.glob("*.sql"))
        self.assertGreaterEqual(len(files), 3)
        for f in files:
            body = f.read_text()
            self.assertIn("SELECT", body.upper())
            self.assertIn("{db}.{table}", body)


if __name__ == "__main__":
    unittest.main()
