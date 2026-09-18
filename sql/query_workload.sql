-- Fixed workload for a future uncached partition/clustering comparison.
-- Run against equivalent full copies of the same generated dataset.
SELECT DATE(created_at, 'UTC') AS order_date, SUM(total_amount) AS revenue_usd
FROM `{{project}}.{{dataset}}.orders`
WHERE created_at >= TIMESTAMP '2026-01-01 00:00:00+00'
  AND created_at < TIMESTAMP '2026-02-01 00:00:00+00'
  AND user_id BETWEEN 1 AND 100
GROUP BY order_date;
