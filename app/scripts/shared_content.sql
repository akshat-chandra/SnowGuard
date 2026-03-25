-- ============================================================================
-- SnowGuard Sample Data
-- ============================================================================
-- Creates sample tables with intentional data quality issues for demo purposes.
-- These tables live in the APPLICATION PACKAGE (provider side) and get shared
-- with the installed app so you always have data to demo against.
-- ============================================================================

-- Create a schema for sample data in the app package
CREATE SCHEMA IF NOT EXISTS snowguard_pkg_akshatchandra.sample_data;

-- Sales pipeline with intentional quality issues
CREATE OR REPLACE TABLE snowguard_pkg_akshatchandra.sample_data.sales_pipeline (
    deal_id NUMBER,
    deal_name VARCHAR,
    account_name VARCHAR,
    stage VARCHAR,
    amount NUMBER(12,2),
    close_date DATE,
    rep_name VARCHAR,
    region VARCHAR,
    created_at TIMESTAMP_NTZ
);

INSERT INTO snowguard_pkg_akshatchandra.sample_data.sales_pipeline VALUES
    -- Good records
    (1, 'Acme Corp Platform Deal', 'Acme Corp', 'Negotiation', 250000.00, '2026-03-15', 'Sarah Chen', 'West', '2026-01-10 09:00:00'),
    (2, 'GlobalTech Expansion', 'GlobalTech Inc', 'Discovery', 180000.00, '2026-04-01', 'James Wilson', 'East', '2026-01-12 10:30:00'),
    (3, 'MedFlow Analytics', 'MedFlow Health', 'Proposal', 320000.00, '2026-03-20', 'Maria Garcia', 'Central', '2026-01-15 14:00:00'),
    (4, 'RetailMax Data Platform', 'RetailMax', 'Closed Won', 475000.00, '2026-02-01', 'Sarah Chen', 'West', '2026-01-05 08:00:00'),
    (5, 'FinServ Cloud Migration', 'FinServ Capital', 'Negotiation', 890000.00, '2026-03-30', 'David Kim', 'East', '2026-01-08 11:00:00'),
    -- Records with NULL issues (missing account, amount, region)
    (6, 'Mystery Deal', NULL, 'Discovery', 150000.00, '2026-04-15', 'James Wilson', 'East', '2026-01-20 09:00:00'),
    (7, 'Budget TBD Project', 'TechStart LLC', 'Qualification', NULL, '2026-05-01', 'Maria Garcia', NULL, '2026-01-22 10:00:00'),
    (8, NULL, 'DataDriven Co', 'Proposal', 200000.00, '2026-03-25', NULL, 'West', '2026-01-25 13:00:00'),
    (9, 'Orphan Deal', NULL, 'Negotiation', NULL, '2026-04-10', 'David Kim', NULL, '2026-02-01 08:30:00'),
    (10, 'Quick Close', 'SpeedCorp', 'Closed Won', 95000.00, NULL, 'Sarah Chen', 'West', '2026-02-03 09:00:00'),
    -- More good records for volume
    (11, 'Enterprise Suite Deal', 'MegaCorp Industries', 'Discovery', 1200000.00, '2026-06-01', 'David Kim', 'East', '2026-02-05 10:00:00'),
    (12, 'SmallBiz Starter', 'Local Shop Inc', 'Closed Won', 25000.00, '2026-02-10', 'James Wilson', 'Central', '2026-01-28 11:00:00'),
    (13, 'Healthcare Analytics', 'City Hospital Group', 'Proposal', 340000.00, '2026-04-20', 'Maria Garcia', 'Central', '2026-02-06 14:30:00'),
    (14, 'Gov Contract Bid', 'State Agency', 'Qualification', 560000.00, '2026-07-01', 'Sarah Chen', 'West', '2026-02-08 08:00:00'),
    (15, 'Startup Pilot', 'NeoTech AI', 'Discovery', 45000.00, '2026-05-15', NULL, 'East', '2026-02-10 09:30:00'),
    -- Duplicate-ish records (same account, similar deal)
    (16, 'Acme Corp Platform Deal - Renewal', 'Acme Corp', 'Qualification', 250000.00, '2026-06-15', 'Sarah Chen', 'West', '2026-02-10 10:00:00'),
    (17, 'GlobalTech Expansion Phase 2', 'GlobalTech Inc', 'Discovery', 180000.00, '2026-07-01', 'James Wilson', 'East', '2026-02-11 10:30:00'),
    -- Record with suspicious amount
    (18, 'Test Deal DO NOT USE', 'Test Account', 'Closed Won', 1.00, '2026-01-01', 'Admin', 'West', '2026-01-01 00:00:00'),
    (19, 'Mega Enterprise', 'BigCo Global', 'Negotiation', 99999999.99, '2026-12-31', 'David Kim', 'East', '2026-02-12 15:00:00'),
    (20, 'Normal Mid-Market', 'MidSize Corp', 'Proposal', 175000.00, '2026-04-30', 'Maria Garcia', 'Central', '2026-02-13 11:00:00');

-- Grant access so the app can read this data
GRANT USAGE ON SCHEMA snowguard_pkg_akshatchandra.sample_data
    TO SHARE IN APPLICATION PACKAGE snowguard_pkg_akshatchandra;
GRANT SELECT ON TABLE snowguard_pkg_akshatchandra.sample_data.sales_pipeline
    TO SHARE IN APPLICATION PACKAGE snowguard_pkg_akshatchandra;

SELECT 'Sample data loaded successfully';
