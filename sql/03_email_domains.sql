-- 03_email_domains.sql — rows per email domain (data-quality view).
SELECT SUBSTR(email, STRPOS(email, '@') + 1) AS domain, COUNT(*) AS n
FROM {db}.{table}
WHERE email LIKE '%@%.%' GROUP BY 1 ORDER BY n DESC LIMIT 10;
