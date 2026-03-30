# SnowGuard — Snowflake Native App for Data Quality Monitoring

**Built with:** Snowflake Native App Framework · Streamlit in Snowflake · Snowflake Cortex · ACCOUNT_USAGE

📹 [Watch the demo](https://drive.google.com/file/d/1z_PMYF6sPhQRMQRLturCE32IJ1rqyWnc/view?usp=drive_link)

---

## What it does

SnowGuard is a data quality monitoring app that installs directly into a Snowflake account. It answers two questions every data team faces:

1. **What is wrong with my data?** — automated checks for null rates, row count trends, schema drift, and value distribution anomalies, with a plain English AI summary powered by Snowflake Cortex.
2. **Where in my pipeline did it break?** — a Lineage Explorer that traces upstream dependencies via `SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES`, cross-referenced with access history and recent query errors to pinpoint the root cause.

---

## Features

### Tab 1 — Run Checks
- **Null Percentage** — scans every column for missing data, shows affected rows
- **Row Count Trend** — flags sudden drops or spikes vs. previous run
- **Schema Drift** — detects added, removed, or renamed columns upstream
- **Value Distribution** — statistical outlier detection for numeric and text columns
- **AI Summary** — Snowflake Cortex generates a plain English explanation of findings
- **Check History** — full audit log of every check run with timestamps and results

### Tab 2 — Scheduled Monitoring
- Add any table and set monitoring frequency (hourly / daily / weekly)
- Runs automatically via Snowflake Tasks
- Pause, resume, or trigger manually on demand

### Tab 3 — Lineage Explorer
- Recursive CTE against `ACCOUNT_USAGE.OBJECT_DEPENDENCIES` — traces up to 10 levels deep
- Access history from `ACCOUNT_USAGE.ACCESS_HISTORY` — flags stale upstream sources
- Recent query errors from `QUERY_HISTORY` — surfaces the exact failure point
- Plain English summary of the dependency chain

---

## Architecture

```
┌─────────────────────────────────────────────┐
│           Snowflake Native App              │
│                                             │
│  Streamlit in Snowflake (UI)                │
│       ↓                                     │
│  Stored Procedures (Python)                 │
│       ↓                                     │
│  ACCOUNT_USAGE views                        │
│  (OBJECT_DEPENDENCIES, ACCESS_HISTORY,      │
│   QUERY_HISTORY)                            │
│       ↓                                     │
│  Snowflake Cortex (AI summaries)            │
└─────────────────────────────────────────────┘
```

Everything runs inside the consumer's Snowflake account — no external APIs, no data leaves the environment.

---

## Project Structure
```
├── snowflake.yml              # Snowflake CLI project definition
├── README.md                  # This file
├── DOCUMENTATION.md           # Detailed running documentation
└── app/
    ├── manifest.yml           # Native App manifest
    ├── README.md              # Consumer-facing readme
    ├── setup_script.sql       # SQL executed on install
    ├── streamlit_app.py       # Streamlit dashboard
    └── scripts/
        └── shared_content.sql # Post-deploy shared content
```

---

## Quick Start

### Prerequisites
- Snowflake account with ACCOUNTADMIN access
- Snowflake CLI (`pip install snowflake-cli-labs`)

```bash
# 1. Configure your connection
snow connection add

# 2. Deploy the app
snow app run

# 3. Open Snowsight → Apps → SNOWGUARD_APP
```

---

## Acknowledgments
Built with assistance from [Claude Code](https://claude.ai/code) by Anthropic.
