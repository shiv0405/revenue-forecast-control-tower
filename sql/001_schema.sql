CREATE TABLE IF NOT EXISTS dim_region (
    region_id SERIAL PRIMARY KEY,
    region_code TEXT UNIQUE NOT NULL,
    region_name TEXT NOT NULL,
    planning_group TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_account (
    account_id TEXT PRIMARY KEY,
    account_name TEXT NOT NULL,
    region_code TEXT NOT NULL REFERENCES dim_region(region_code),
    segment TEXT NOT NULL,
    product_family TEXT NOT NULL,
    contract_value_usd NUMERIC(14, 2) NOT NULL,
    renewal_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_daily_bookings (
    booking_date DATE NOT NULL,
    region_code TEXT NOT NULL REFERENCES dim_region(region_code),
    account_id TEXT NOT NULL REFERENCES dim_account(account_id),
    product_family TEXT NOT NULL,
    booking_amount_usd NUMERIC(14, 2) NOT NULL,
    target_amount_usd NUMERIC(14, 2) NOT NULL,
    discount_pct NUMERIC(6, 3) NOT NULL,
    channel TEXT NOT NULL,
    PRIMARY KEY (booking_date, account_id, product_family)
);

CREATE TABLE IF NOT EXISTS fact_pipeline_snapshot (
    snapshot_date DATE NOT NULL,
    opportunity_id TEXT NOT NULL,
    account_id TEXT NOT NULL REFERENCES dim_account(account_id),
    region_code TEXT NOT NULL REFERENCES dim_region(region_code),
    stage_name TEXT NOT NULL,
    weighted_pipeline_usd NUMERIC(14, 2) NOT NULL,
    commit_probability NUMERIC(6, 3) NOT NULL,
    expected_close_date DATE NOT NULL,
    slippage_risk TEXT NOT NULL,
    PRIMARY KEY (snapshot_date, opportunity_id)
);

CREATE TABLE IF NOT EXISTS fact_product_usage (
    usage_date DATE NOT NULL,
    account_id TEXT NOT NULL REFERENCES dim_account(account_id),
    active_users INTEGER NOT NULL,
    weekly_sessions INTEGER NOT NULL,
    support_tickets INTEGER NOT NULL,
    satisfaction_score NUMERIC(5, 2) NOT NULL,
    PRIMARY KEY (usage_date, account_id)
);
