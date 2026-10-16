-- 02_by_city.sql — rows per city, most common first.
SELECT city, COUNT(*) AS n FROM {db}.{table}
GROUP BY city ORDER BY n DESC LIMIT 10;
