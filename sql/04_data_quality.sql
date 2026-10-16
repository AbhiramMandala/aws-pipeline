-- 04_data_quality.sql — rows that still need attention after cleaning.
-- Empty emails are expected (cleaner nulls invalid ones); anything here with
-- a missing id or name indicates a source problem, since those rows are dropped.
SELECT id, name, email, city FROM {db}.{table}
WHERE id IS NULL OR id = '' OR name IS NULL OR name = ''
LIMIT 100;
