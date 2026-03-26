INSERT INTO dim_region (region_code, region_name, planning_group)
VALUES
    ('NA', 'North America', 'AMER'),
    ('EU', 'Europe', 'EMEA'),
    ('AP', 'Asia Pacific', 'APAC'),
    ('LA', 'Latin America', 'LATAM'),
    ('ME', 'Middle East', 'EMEA'),
    ('AF', 'Africa', 'EMEA')
ON CONFLICT (region_code) DO NOTHING;
