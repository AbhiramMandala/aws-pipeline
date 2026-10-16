-- 01_row_counts.sql — total cleaned rows.
-- Replace {db} and {table} with your Glue database/table, or run in Athena
-- with the query result location set to s3://<bucket>/athena-results/.
SELECT COUNT(*) AS n FROM {db}.{table};
