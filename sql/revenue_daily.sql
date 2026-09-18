-- Grain: UTC order date × current product category. One product per demo order.
CREATE OR REPLACE VIEW `{{project}}.{{dataset}}.revenue_daily` AS
SELECT
  DATE(o.created_at, 'UTC') AS order_date,
  p.category AS current_product_category,
  COUNTIF(o.total_amount >= 0) AS order_count,
  COUNTIF(o.total_amount < 0) AS refund_count,
  SUM(IF(o.total_amount >= 0, o.total_amount, NUMERIC '0')) AS gross_revenue_usd,
  -SUM(IF(o.total_amount < 0, o.total_amount, NUMERIC '0')) AS refunds_usd,
  SUM(o.total_amount) AS net_revenue_usd,
  SAFE_DIVIDE(SUM(IF(o.total_amount >= 0, o.total_amount, NUMERIC '0')),
              COUNTIF(o.total_amount >= 0)) AS average_order_value_usd
FROM `{{project}}.{{dataset}}.orders` o
JOIN `{{project}}.{{dataset}}.products` p USING (product_id)
GROUP BY order_date, current_product_category;
