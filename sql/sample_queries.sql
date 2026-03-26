-- Quarterly forecast performance by region
SELECT
    region_code,
    quarter_start,
    bookings_usd,
    target_usd,
    attainment_ratio
FROM vw_regional_forecast
ORDER BY quarter_start DESC, region_code;

-- Accounts with the largest weighted pipeline and high slippage risk
SELECT
    account_id,
    region_code,
    weighted_pipeline_usd,
    avg_commit_probability,
    slippage_risk
FROM vw_pipeline_risk
WHERE slippage_risk = 'high'
ORDER BY weighted_pipeline_usd DESC
LIMIT 25;
