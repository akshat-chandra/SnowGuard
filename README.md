# SnowGuard — Snowflake Native App for Data Quality Monitoring

## Overview
SnowGuard is a Snowflake Native App that installs directly into a consumer's Snowflake account and monitors data quality. Built to demonstrate the Native App Framework, Streamlit in Snowflake, stored procedures, and the Shareback feature.

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

## Prerequisites
- Snowflake account with ACCOUNTADMIN access
- Snowflake CLI (`pip install snowflake-cli-labs`)

## Quick Start
```bash
# 1. Configure your connection
snow connection add

# 2. Deploy the app
snow app run

# 3. Open Snowsight and navigate to Apps > SNOWGUARD_APP
```

## Acknowledgments
Built with assistance from [Claude Code](https://claude.ai/code) by Anthropic.
