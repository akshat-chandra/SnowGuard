# SnowGuard — Data Quality Monitor

A Snowflake Native App that monitors your data quality — runs entirely inside your Snowflake account, no external infrastructure needed.

---

## Table of Contents
1. [What Is This?](#what-is-this)
2. [Features](#features)
3. [Architecture](#architecture)
4. [How It Works](#how-it-works)
5. [Snowflake vs Databricks](#snowflake-vs-databricks)
6. [Why Snowflake Still Matters (vs AI)](#why-snowflake-still-matters-vs-ai)
7. [File Structure](#file-structure)
8. [Deployment](#deployment)

---

## What Is This?

**SnowGuard** is a data quality monitoring tool that:
- Checks your tables for problems (nulls, row count drops, schema changes, outliers)
- Runs **inside** your Snowflake account (data never leaves)
- Has a Streamlit dashboard for visualization
- Can be scheduled to run automatically

**Think of it like:** A health monitor for your database tables.

---

## Features

| Feature | What It Does | Why It Matters |
|---------|--------------|----------------|
| **Null Percentage** | Counts nulls in every column | Catches missing data before reports break |
| **Row Count** | Compares row count to previous run | Detects failed pipelines (sudden drops) |
| **Schema Drift** | Tracks column additions/removals | Catches breaking changes early |
| **Value Distribution** | Stats + outlier detection (CV > 3) | Finds suspicious values ($99M typo) |
| **Check History** | Stores all results with timestamps | Audit trail for compliance |
| **Scheduled Monitoring** | Add tables to auto-check list | Set it and forget it |
| **Cortex AI Summary** | Plain-English result explanation | No SQL knowledge needed to understand |
| **Streamlit Dashboard** | Visual UI with tabs and controls | Easy for non-technical users |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     CUSTOMER'S SNOWFLAKE ACCOUNT                        │
│                                                                         │
│  ┌─────────────────┐      ┌─────────────────────────────────────────┐  │
│  │                 │      │           SNOWGUARD APP                 │  │
│  │   CUSTOMER'S    │      │                                         │  │
│  │   DATA TABLES   │◄────►│  ┌─────────────┐  ┌──────────────────┐  │  │
│  │                 │      │  │   STORED    │  │    STREAMLIT     │  │  │
│  │  - sales       │      │  │  PROCEDURES │  │    DASHBOARD     │  │  │
│  │  - customers   │      │  │             │  │                  │  │  │
│  │  - orders      │      │  │ - null check│  │  [Run Checks]    │  │  │
│  │                 │      │  │ - row count │  │  [Scheduling]    │  │  │
│  └─────────────────┘      │  │ - schema    │  │  [History]       │  │  │
│                           │  │ - distrib.  │  │                  │  │  │
│                           │  └─────────────┘  └──────────────────┘  │  │
│                           │                                         │  │
│                           │  ┌─────────────┐  ┌──────────────────┐  │  │
│                           │  │   RESULTS   │  │     CONFIG       │  │  │
│                           │  │   TABLE     │  │     TABLE        │  │  │
│                           │  │ (history)   │  │ (monitored tbls) │  │  │
│                           │  └─────────────┘  └──────────────────┘  │  │
│                           └─────────────────────────────────────────┘  │
│                                                                         │
│   KEY: Data NEVER leaves this box. Everything runs inside Snowflake.   │
└─────────────────────────────────────────────────────────────────────────┘
```

### Components

| Component | What It Is | Location |
|-----------|------------|----------|
| **Stored Procedures** | SQL code that runs the checks | `SNOWGUARD.CORE.*` |
| **Results Table** | Stores check history | `SNOWGUARD.RESULTS.QUALITY_CHECKS` |
| **Config Table** | Tables being monitored | `SNOWGUARD.CONFIG.MONITORED_TABLES` |
| **Streamlit App** | Visual dashboard | `SNOWGUARD.UI.SNOWGUARD_DASHBOARD` |

---

## How It Works

### Step 1: Install
```
Customer clicks "Install" on Snowflake Marketplace
    → App installs into their account
    → Setup script creates schemas, procedures, tables
    → Streamlit dashboard becomes available
```

### Step 2: Run Checks
```
User opens dashboard → Selects table → Clicks "Run Checks"
    → Procedures execute SQL against the table
    → Results stored in history table
    → Dashboard shows pass/warn/fail with details
```

### Step 3: Schedule (Optional)
```
User adds tables to monitoring list
    → Creates a Task to run hourly/daily
    → Checks run automatically
    → History accumulates for trend analysis
```

### The Data Quality Checks

**1. Null Percentage**
```sql
-- For each column, calculates:
SELECT COUNT(*) FILTER (WHERE column IS NULL) / COUNT(*) * 100
-- Flags: >10% = WARN, >30% = FAIL
```

**2. Row Count**
```sql
-- Compares to previous run:
Current: 10,000 rows
Previous: 10,500 rows
Change: -5% → PASS

Current: 100 rows
Previous: 10,000 rows  
Change: -99% → FAIL (pipeline probably broke)
```

**3. Schema Drift**
```sql
-- Snapshots columns, compares to last snapshot:
Previous: [id, name, email, created_at]
Current:  [id, name, phone, created_at]

Result: WARN - "email" removed, "phone" added
```

**4. Value Distribution**
```sql
-- For numeric columns:
AMOUNT: min=$10, max=$500, avg=$127, stddev=$45
CV (coefficient of variation) = 0.35 → PASS

AMOUNT: min=$10, max=$99,999,999, avg=$5M, stddev=$20M
CV = 4.0 → WARN (that $99M is probably a typo)
```

---

## Snowflake vs Databricks

### The Key Difference

**Snowflake Native Apps:** App runs WHERE the data lives
**Databricks + External Tools:** Data moves TO where the app lives

```
SNOWFLAKE NATIVE APPS:
┌────────────────────────────────────┐
│  Customer's Snowflake Account      │
│                                    │
│   DATA ◄──── APP                   │
│   (stays)    (installed here)      │
│                                    │
│   Nothing leaves this box.         │
└────────────────────────────────────┘


DATABRICKS + EXTERNAL TOOL:
┌────────────────────────────────────┐        ┌─────────────────────┐
│  Customer's Databricks Account     │        │  Vendor's Cloud     │
│                                    │        │  (Monte Carlo, etc) │
│   DATA ─────────────────────────────────►   │                     │
│        (extracted via API)         │        │  Processing here    │
│                                    │        │                     │
└────────────────────────────────────┘        └─────────────────────┘
```

### Why This Matters

| Concern | Snowflake Native Apps | Databricks + External |
|---------|----------------------|----------------------|
| **Security** | Data never leaves | Data sent to vendor |
| **Compliance** | Easy (no data transfer) | Hard (legal review needed) |
| **Setup time** | 5 minutes | 2-3 months |
| **Cost** | Pay through Snowflake | Separate vendor contract |
| **Latency** | Zero (same environment) | API calls over network |

### Real Example

**A bank needs data quality monitoring:**

**Databricks path:**
1. Evaluate vendors (Monte Carlo, Atlan) — 2 weeks
2. Security review of vendor's SOC2 — 3 weeks
3. Legal reviews data processing agreement — 2 weeks
4. IT sets up API connections — 1 week
5. Data gets pulled to vendor's cloud
6. Compliance team nervous about data residency
7. **Total: 2-3 months, $50K+/year**

**Snowflake path:**
1. Find SnowGuard on Marketplace
2. Click "Install"
3. Grant access to tables
4. **Total: 5 minutes, data never leaves**

---

## Why Snowflake Still Matters (vs AI)

### The Question
> "If AI (Claude, ChatGPT) can do everything, why does Snowflake matter?"

### The Answer

**AI doesn't replace data infrastructure. It makes it MORE important.**

| AI Does This | Snowflake Does This |
|--------------|---------------------|
| Generates code | **Stores** the actual data |
| Answers questions | **Secures** access to data |
| Runs in vendor's cloud | Runs in **YOUR** cloud |
| Needs data sent to it | Keeps data **in place** |

### Why Data Staying In Place Matters More Now

1. **AI needs your data** — LLMs are useless without company data to work with
2. **Data is your moat** — Your proprietary data is your competitive advantage
3. **Regulations are tightening** — GDPR, HIPAA, SOC2, data residency laws
4. **Breaches are costly** — Less data movement = less attack surface

### Snowflake's Strategy

**Be the platform where everything lives together:**

```
┌─────────────────────────────────────────────────┐
│              SNOWFLAKE PLATFORM                 │
│                                                 │
│   DATA          APPS           AI              │
│   (tables)      (Native Apps)  (Cortex)        │
│      │              │             │             │
│      └──────────────┴─────────────┘             │
│            All in one place                     │
│            No data movement                     │
│            One governance model                 │
└─────────────────────────────────────────────────┘
```

### What SnowGuard Demonstrates

| Snowflake Capability | How SnowGuard Uses It |
|---------------------|----------------------|
| **Native App Framework** | Installs inside customer account |
| **Streamlit in Snowflake** | Dashboard with zero infrastructure |
| **Cortex AI** | LLM-powered summaries |
| **SQL Scripting** | Complex logic in procedures |
| **Marketplace** | One-click distribution |

### One-Liner

> "SnowGuard monitors data quality **inside** Snowflake — data never leaves, 
> no external infrastructure, showcasing why Snowflake is a **platform**, 
> not just a warehouse."

---

## File Structure

```
Snowflake/
├── snowflake.yml              # Project config for Snowflake CLI
├── DOCUMENTATION.md           # This file
└── app/
    ├── manifest.yml           # App metadata + required privileges
    ├── setup_script.sql       # Creates schemas, procedures, tables (732 lines)
    ├── streamlit_app.py       # Dashboard UI (362 lines)
    ├── README.md              # Marketplace listing description
    └── scripts/
        └── shared_content.sql # Sample data with intentional quality issues
```

### Key Files Explained

**`setup_script.sql`** — The brain of the app
- Creates 4 schemas: `core`, `results`, `config`, `ui`
- Creates 11 stored procedures for all the checks
- Creates tables for history and config
- Runs when app is installed

**`streamlit_app.py`** — The face of the app
- Two tabs: "Run Checks" and "Scheduled Monitoring"
- Calls stored procedures, displays results
- Color-coded status badges (green/orange/red)

**`manifest.yml`** — The permissions
- Declares what the app needs (access to SNOWFLAKE DB for metadata)
- Points to setup script and Streamlit file

---

## Deployment

### Prerequisites
- Snowflake account (free trial works)
- Snowflake CLI installed (`pip install snowflake-cli-labs`)
- ACCOUNTADMIN role

### Deploy
```bash
cd Snowflake
snow app run --connection <your-connection>
```

### Access
After deployment:
1. Open Snowsight
2. Go to Apps → SNOWGUARD_<your_username>
3. Open the Streamlit dashboard

### Set Warehouse for Streamlit
```sql
-- Required for Streamlit to run queries
ALTER STREAMLIT SNOWGUARD_APP.UI.SNOWGUARD_DASHBOARD 
SET QUERY_WAREHOUSE = COMPUTE_WH;
```

### Create Scheduled Task (Optional)
```sql
-- Consumer creates this to run checks automatically
CREATE TASK snowguard_daily
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 8 * * * UTC'  -- 8 AM daily
AS CALL SNOWGUARD_APP.CORE.RUN_SCHEDULED_CHECKS();

ALTER TASK snowguard_daily RESUME;
```

---

## Quick Reference

### Procedures

| Procedure | What It Does |
|-----------|--------------|
| `core.check_null_percentage(db, schema, table)` | Null % per column |
| `core.check_row_count(db, schema, table)` | Row count vs previous |
| `core.check_schema_drift(db, schema, table)` | Column changes |
| `core.check_value_distribution(db, schema, table)` | Stats + outliers |
| `core.run_all_checks(db, schema, table)` | Runs all 4 above |
| `core.run_all_checks_with_summary(db, schema, table)` | All 4 + AI summary |
| `core.add_monitored_table(db, schema, table, freq)` | Add to schedule |
| `core.remove_monitored_table(db, schema, table)` | Remove from schedule |
| `core.run_scheduled_checks()` | Run all monitored tables |
| `core.get_monitored_tables()` | List monitored tables |
| `core.get_check_history(db, schema, table)` | Get past results |

### Thresholds

| Check | WARN | FAIL |
|-------|------|------|
| Null % | > 10% | > 30% |
| Row Count Change | > 50% | > 80% |
| Schema Drift | Columns added | Columns removed |
| Value Distribution | CV > 3 | — |

---

**Version:** 0.3.0  
**App URL:** https://app.snowflake.com/BLCCGBW/bf58475/#/apps/application/SNOWGUARD_AKSHATCHANDRA
