CREATE OR REPLACE VIEW vw_regional_forecast AS
SELECT
    b.region_code,
    DATE_TRUNC('quarter', b.booking_date)::date AS quarter_start,
    SUM(b.booking_amount_usd) AS bookings_usd,
    SUM(b.target_amount_usd) AS target_usd,
    ROUND(SUM(b.booking_amount_usd) / NULLIF(SUM(b.target_amount_usd), 0), 4) AS attainment_ratio
FROM fact_daily_bookings b
GROUP BY 1, 2;

CREATE OR REPLACE VIEW vw_pipeline_risk AS
SELECT
    p.region_code,
    p.account_id,
    MAX(p.snapshot_date) AS latest_snapshot_date,
    SUM(p.weighted_pipeline_usd) AS weighted_pipeline_usd,
    AVG(p.commit_probability) AS avg_commit_probability,
    MAX(p.slippage_risk) AS slippage_risk
FROM fact_pipeline_snapshot p
GROUP BY 1, 2;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_exec_kpis AS
SELECT
    CURRENT_DATE AS snapshot_date,
    COUNT(DISTINCT a.account_id) AS accounts_in_scope,
    SUM(a.contract_value_usd) AS total_contract_value_usd,
    AVG(u.satisfaction_score) AS avg_satisfaction_score,
    SUM(CASE WHEN p.slippage_risk = 'high' THEN 1 ELSE 0 END) AS high_pipeline_accounts
FROM dim_account a
LEFT JOIN fact_product_usage u ON u.account_id = a.account_id
LEFT JOIN vw_pipeline_risk p ON p.account_id = a.account_id
GROUP BY 1;
