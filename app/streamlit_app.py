import streamlit as st
import json
from snowflake.snowpark.context import get_active_session

session = get_active_session()

# -- Page Config --
st.set_page_config(page_title="SnowGuard", layout="wide")

# -- Header --
st.title("SnowGuard: Data Quality Monitor")
st.caption("A Snowflake Native App for automated data quality checks")

# -- Tabs for different sections --
tab1, tab2, tab3 = st.tabs(["Run Checks", "Scheduled Monitoring", "Lineage Explorer"])

# -- Helper to parse VARIANT results --
def parse_result(raw):
    """Parse a Snowflake VARIANT result into a Python dict."""
    if raw is None:
        return {}
    val = raw[0][0] if raw else None
    if val is None:
        return {}
    if isinstance(val, str):
        return json.loads(val)
    return val


def status_badge(status):
    """Return a colored status indicator."""
    if status == "pass":
        return ":green[PASS]"
    elif status == "warn":
        return ":orange[WARN]"
    elif status == "fail":
        return ":red[FAIL]"
    return status


# ============================================================================
# TAB 1: Run Checks
# ============================================================================
with tab1:
    # -- Sidebar: Table Selection --
    st.sidebar.header("Select a Table")

    # Let user input database, schema, table
    db_name = st.sidebar.text_input("Database", value="SNOWGUARD_PKG_AKSHATCHANDRA")
    schema_name = st.sidebar.text_input("Schema", value="SAMPLE_DATA")
    tbl_name = st.sidebar.text_input("Table", value="SALES_PIPELINE")

    full_table = f"{db_name}.{schema_name}.{tbl_name}"

    st.sidebar.divider()
    st.sidebar.header("Select Checks")
    run_nulls = st.sidebar.checkbox("Null Percentage", value=True)
    run_rowcount = st.sidebar.checkbox("Row Count Trend", value=True)
    run_schema = st.sidebar.checkbox("Schema Drift", value=True)
    run_distribution = st.sidebar.checkbox("Value Distribution", value=True)
    
    st.sidebar.divider()
    include_ai_summary = st.sidebar.checkbox("Include AI Summary", value=True, 
        help="Generate a plain-English summary using Cortex AI")

    # Store results for AI summary
    all_results = {}

    # -- Run Checks --
    if st.sidebar.button("Run Selected Checks", type="primary", use_container_width=True):

        st.subheader(f"Results for `{full_table}`")

        # ---- Null Percentage ----
        if run_nulls:
            with st.spinner("Running null percentage check..."):
                try:
                    result = session.sql(
                        f"CALL core.check_null_percentage('{db_name}', '{schema_name}', '{tbl_name}')"
                    ).collect()
                    data = parse_result(result)
                    all_results['null_percentage'] = data

                    st.markdown(f"### Null Percentage {status_badge(data.get('result', 'unknown'))}")
                    st.caption(f"Total rows: {data.get('total_rows', 'N/A')}")

                    columns = data.get("columns", [])
                    if columns:
                        # Build a table for display
                        col_data = []
                        for col in columns:
                            col_data.append({
                                "Column": col.get("column_name", ""),
                                "Null Count": int(col.get("null_count", 0)),
                                "Null %": float(col.get("null_pct", 0)),
                                "Status": col.get("status", "").upper()
                            })
                        st.dataframe(col_data, use_container_width=True, hide_index=True)

                        # Show the actual rows that have missing data
                        null_cols_affected = [c for c in columns if int(c.get("null_count", 0)) > 0]
                        if null_cols_affected:
                            conditions = " OR ".join([f'"{c["column_name"]}" IS NULL' for c in null_cols_affected])
                            with st.expander(f"View rows with missing data ({len(null_cols_affected)} column(s) affected)"):
                                try:
                                    bad_rows = session.sql(f"SELECT * FROM {full_table} WHERE {conditions}").collect()
                                    if bad_rows:
                                        st.dataframe([row.as_dict() for row in bad_rows], use_container_width=True, hide_index=True)
                                    else:
                                        st.info("No rows with missing data found.")
                                except Exception as ex:
                                    st.warning(f"Could not fetch rows: {ex}")
                    st.divider()
                except Exception as e:
                    st.error(f"Null check failed: {e}")

        # ---- Row Count ----
        if run_rowcount:
            with st.spinner("Running row count check..."):
                try:
                    result = session.sql(
                        f"CALL core.check_row_count('{db_name}', '{schema_name}', '{tbl_name}')"
                    ).collect()
                    data = parse_result(result)
                    all_results['row_count'] = data

                    st.markdown(f"### Row Count {status_badge(data.get('result', 'unknown'))}")

                    col1, col2, col3 = st.columns(3)
                    col1.metric("Current Rows", f"{data.get('row_count', 'N/A'):,}" if isinstance(data.get('row_count'), (int, float)) else "N/A")
                    prev = data.get("previous_count")
                    col2.metric("Previous Rows", f"{prev:,}" if isinstance(prev, (int, float)) else "First run")
                    col3.metric("Change %", f"{data.get('pct_change', 0)}%")
                    st.divider()
                except Exception as e:
                    st.error(f"Row count check failed: {e}")

        # ---- Schema Drift ----
        if run_schema:
            with st.spinner("Running schema drift check..."):
                try:
                    result = session.sql(
                        f"CALL core.check_schema_drift('{db_name}', '{schema_name}', '{tbl_name}')"
                    ).collect()
                    data = parse_result(result)
                    all_results['schema_drift'] = data

                    st.markdown(f"### Schema Drift {status_badge(data.get('result', 'unknown'))}")

                    drift = data.get("drift", {})
                    if drift.get("message"):
                        st.info(drift["message"])
                    else:
                        added = drift.get("columns_added", [])
                        removed = drift.get("columns_removed", [])
                        if added:
                            st.warning(f"Columns added: {', '.join(added)}")
                        if removed:
                            st.error(f"Columns removed: {', '.join(removed)}")
                        if not added and not removed:
                            st.success("No schema changes detected.")

                    # Show current schema
                    current_schema = data.get("current_schema", [])
                    if current_schema:
                        schema_display = []
                        for col in current_schema:
                            schema_display.append({
                                "Column": col.get("column_name", ""),
                                "Type": col.get("data_type", ""),
                                "Position": col.get("ordinal_position", ""),
                                "Nullable": col.get("is_nullable", "")
                            })
                        with st.expander("View Current Schema"):
                            st.dataframe(schema_display, use_container_width=True, hide_index=True)
                    st.divider()
                except Exception as e:
                    st.error(f"Schema drift check failed: {e}")

        # ---- Value Distribution ----
        if run_distribution:
            with st.spinner("Running value distribution check..."):
                try:
                    result = session.sql(
                        f"CALL core.check_value_distribution('{db_name}', '{schema_name}', '{tbl_name}')"
                    ).collect()
                    data = parse_result(result)
                    all_results['value_distribution'] = data

                    st.markdown(f"### Value Distribution {status_badge(data.get('result', 'unknown'))}")

                    distributions = data.get("distributions", [])
                    numeric_cols = [d for d in distributions if d.get("data_type") == "NUMERIC"]
                    varchar_cols = [d for d in distributions if d.get("data_type") == "VARCHAR"]

                    if numeric_cols:
                        st.markdown("**Numeric Columns**")
                        num_data = []
                        for col in numeric_cols:
                            num_data.append({
                                "Column": col.get("column_name", ""),
                                "Min": col.get("min"),
                                "Max": col.get("max"),
                                "Avg": col.get("avg"),
                                "Std Dev": col.get("stddev"),
                                "Distinct": col.get("distinct_count"),
                                "CV": col.get("coefficient_of_variation")
                            })
                        st.dataframe(num_data, use_container_width=True, hide_index=True)

                        # Show outlier rows for any column with CV > 3
                        outlier_cols = [c for c in numeric_cols if float(c.get("coefficient_of_variation") or 0) > 3]
                        for oc in outlier_cols:
                            col_nm = oc.get("column_name")
                            avg_val = float(oc.get("avg") or 0)
                            std_val = float(oc.get("stddev") or 0)
                            if std_val > 0:
                                with st.expander(f"View outlier rows — {col_nm}"):
                                    try:
                                        outlier_q = (
                                            f'SELECT * FROM {full_table} '
                                            f'WHERE ABS("{col_nm}" - {avg_val}) > 2 * {std_val} '
                                            f'AND "{col_nm}" IS NOT NULL'
                                        )
                                        outlier_rows = session.sql(outlier_q).collect()
                                        if outlier_rows:
                                            st.dataframe([row.as_dict() for row in outlier_rows], use_container_width=True, hide_index=True)
                                        else:
                                            st.info("No outlier rows found.")
                                    except Exception as ex:
                                        st.warning(f"Could not fetch outlier rows: {ex}")

                    if varchar_cols:
                        st.markdown("**Text Columns**")
                        for col in varchar_cols:
                            with st.expander(f"{col.get('column_name', '')} ({col.get('distinct_count', '?')} distinct values)"):
                                top_vals = col.get("top_values", [])
                                if top_vals:
                                    top_data = [{"Value": v.get("value", ""), "Count": v.get("count", 0)} for v in top_vals]
                                    st.dataframe(top_data, use_container_width=True, hide_index=True)
                    st.divider()
                except Exception as e:
                    st.error(f"Value distribution check failed: {e}")

        # ---- AI Summary ----
        if include_ai_summary and all_results:
            st.markdown("### AI Summary")
            with st.spinner("Generating AI summary with Cortex..."):
                try:
                    # Convert results to JSON for the AI
                    results_json = json.dumps({
                        'table': full_table,
                        **all_results
                    })
                    
                    # Call the AI summary procedure
                    ai_result = session.sql(
                        f"CALL core.generate_ai_summary(PARSE_JSON('{results_json.replace(chr(39), chr(39)+chr(39))}'))"
                    ).collect()
                    
                    ai_summary = ai_result[0][0] if ai_result else "No summary generated."
                    
                    st.info(ai_summary)
                except Exception as e:
                    st.warning(f"AI summary unavailable: {e}")

    # -- Check History --
    st.subheader("Check History")
    try:
        history = session.sql(
            f"CALL core.get_check_history('{db_name}', '{schema_name}', '{tbl_name}')"
        ).collect()
        if history:
            history_data = []
            for row in history:
                history_data.append({
                    "ID": row[0],
                    "Timestamp": row[1],
                    "Check Type": row[2],
                    "Result": row[3]
                })
            st.dataframe(history_data, use_container_width=True, hide_index=True)
        else:
            st.info("No check history yet. Run some checks to populate this table.")
    except Exception as e:
        st.info("No check history yet. Run some checks to populate this table.")


# ============================================================================
# TAB 2: Scheduled Monitoring
# ============================================================================
with tab2:
    st.subheader("Scheduled Monitoring")
    st.caption("Add tables to be automatically monitored on a schedule")
    
    # -- Add Table to Monitoring --
    st.markdown("#### Add Table to Monitoring")
    col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
    
    with col1:
        new_db = st.text_input("Database", key="new_db", placeholder="MY_DATABASE")
    with col2:
        new_schema = st.text_input("Schema", key="new_schema", placeholder="MY_SCHEMA")
    with col3:
        new_table = st.text_input("Table", key="new_table", placeholder="MY_TABLE")
    with col4:
        new_freq = st.selectbox("Frequency", ["HOURLY", "DAILY", "WEEKLY"], key="new_freq")
    
    if st.button("Add to Monitoring", type="primary"):
        if new_db and new_schema and new_table:
            try:
                result = session.sql(
                    f"CALL core.add_monitored_table('{new_db}', '{new_schema}', '{new_table}', '{new_freq}')"
                ).collect()
                st.success(result[0][0])
                st.rerun()
            except Exception as e:
                st.error(f"Failed to add table: {e}")
        else:
            st.warning("Please fill in all fields.")
    
    st.divider()
    
    # -- Currently Monitored Tables --
    st.markdown("#### Currently Monitored Tables")
    
    try:
        monitored = session.sql("CALL core.get_monitored_tables()").collect()
        
        if monitored:
            for row in monitored:
                tbl_id, db, sch, tbl, freq, enabled, last_check = row
                full_name = f"{db}.{sch}.{tbl}"
                
                col1, col2, col3, col4 = st.columns([4, 2, 2, 2])
                
                with col1:
                    status_icon = "🟢" if enabled else "🔴"
                    st.markdown(f"{status_icon} **{full_name}**")
                
                with col2:
                    st.caption(f"Frequency: {freq}")
                
                with col3:
                    if last_check:
                        st.caption(f"Last: {last_check}")
                    else:
                        st.caption("Never checked")
                
                with col4:
                    btn_col1, btn_col2 = st.columns(2)
                    with btn_col1:
                        if enabled:
                            if st.button("Pause", key=f"pause_{tbl_id}", use_container_width=True):
                                session.sql(f"CALL core.toggle_monitoring('{db}', '{sch}', '{tbl}', FALSE)").collect()
                                st.rerun()
                        else:
                            if st.button("Resume", key=f"resume_{tbl_id}", use_container_width=True):
                                session.sql(f"CALL core.toggle_monitoring('{db}', '{sch}', '{tbl}', TRUE)").collect()
                                st.rerun()
                    with btn_col2:
                        if st.button("Remove", key=f"remove_{tbl_id}", use_container_width=True):
                            session.sql(f"CALL core.remove_monitored_table('{db}', '{sch}', '{tbl}')").collect()
                            st.rerun()
                
                st.divider()
        else:
            st.info("No tables are currently being monitored. Add a table above to get started.")
            
    except Exception as e:
        st.info("No tables are currently being monitored. Add a table above to get started.")
    
    # -- Run Scheduled Checks Manually --
    st.markdown("#### Manual Run")
    st.caption("Trigger all scheduled checks immediately")
    
    if st.button("Run All Scheduled Checks Now"):
        with st.spinner("Running scheduled checks..."):
            try:
                result = session.sql("CALL core.run_scheduled_checks()").collect()
                data = parse_result(result)
                
                st.success(f"Checked {data.get('tables_checked', 0)} table(s)")
                
                results = data.get('results', [])
                for r in results:
                    if r.get('status') == 'success':
                        st.write(f"✅ {r.get('table')}")
                    else:
                        st.write(f"❌ {r.get('table')}: {r.get('error', 'Unknown error')}")
            except Exception as e:
                st.error(f"Failed to run scheduled checks: {e}")


# ============================================================================
# TAB 3: Lineage Explorer
# ============================================================================
with tab3:
    st.subheader("Lineage Explorer")
    st.caption(
        "Traces upstream dependencies via `SNOWFLAKE.ACCOUNT_USAGE` — "
        "pinpoints **where** a breakdown originated, not just what the error was."
    )

    investigate_obj = st.text_input(
        "Object to investigate (table or view name)",
        value="SALES_PIPELINE",
    ).upper()

    if st.button("Trace Lineage", type="primary"):

        # -- Upstream dependency chain --
        with st.spinner("Walking dependency graph..."):
            try:
                lineage_sql = f"""
                WITH RECURSIVE upstream AS (
                    SELECT downstream, upstream, upstream_type, 1 AS depth
                    FROM (
                        SELECT DISTINCT
                            referencing_object_name  AS downstream,
                            referenced_object_name   AS upstream,
                            referenced_object_domain AS upstream_type
                        FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
                    ) base
                    WHERE downstream = '{investigate_obj}'

                    UNION ALL

                    SELECT d.downstream, d.upstream, d.upstream_type, u.depth + 1
                    FROM (
                        SELECT DISTINCT
                            referencing_object_name  AS downstream,
                            referenced_object_name   AS upstream,
                            referenced_object_domain AS upstream_type
                        FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
                    ) d
                    INNER JOIN upstream u ON d.downstream = u.upstream
                    WHERE u.depth < 10
                )
                SELECT DISTINCT depth, downstream, upstream, upstream_type FROM upstream
                ORDER BY depth, upstream
                """
                lineage_rows = session.sql(lineage_sql).collect()

                if not lineage_rows:
                    st.warning(
                        f"No upstream dependencies found for `{investigate_obj}`. "
                        "Check the object name, or it may have no tracked dependencies yet."
                    )
                else:
                    # Plain English summary for non-technical users
                    direct_sources = [r for r in lineage_rows if r[0] == 1]
                    deeper_sources = [r for r in lineage_rows if r[0] > 1]
                    direct_names = list({r[2] for r in direct_sources})
                    max_depth = max(r[0] for r in lineage_rows)

                    st.info(
                        f"**{investigate_obj}** pulls data from "
                        f"**{len(direct_names)} source(s)**: {', '.join(direct_names)}. "
                        f"If this report looks wrong or broken, one of these sources is where to look first. "
                        + (f"Tracing further back, the chain goes **{max_depth} levels deep** — "
                           f"meaning there are {len({r[2] for r in deeper_sources})} additional upstream table(s) "
                           f"that ultimately feed into it." if deeper_sources else "")
                    )

                    with st.expander("See full dependency chain"):
                        lineage_data = [
                            {
                                "Depth": r[0],
                                "Downstream Object": r[1],
                                "Upstream Source": r[2],
                                "Source Type": r[3],
                            }
                            for r in lineage_rows
                        ]
                        st.dataframe(lineage_data, use_container_width=True, hide_index=True)

            except Exception as e:
                st.error(f"Lineage query failed: {e}")
                st.info("ACCOUNT_USAGE requires ACCOUNTADMIN or the SNOWFLAKE database privilege.")
                lineage_rows = []

        # -- Last access times per upstream source --
        if lineage_rows:
            with st.spinner("Checking last access times..."):
                try:
                    access_sql = """
                    SELECT
                        obj.value:objectName::STRING AS object_name,
                        MAX(query_start_time)        AS last_accessed_at
                    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
                         LATERAL FLATTEN(input => base_objects_accessed) obj
                    WHERE query_start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP)
                    GROUP BY 1
                    """
                    access_rows = session.sql(access_sql).collect()
                    access_map = {r[0]: r[1] for r in access_rows}

                    st.markdown("### When were the sources last active?")
                    seen = set()
                    access_display = []
                    stale = []
                    for r in lineage_rows:
                        upstream = r[2]
                        if upstream in seen:
                            continue
                        seen.add(upstream)
                        last_seen = next(
                            (v for k, v in access_map.items() if upstream in k),
                            None
                        )
                        label = str(last_seen) if last_seen else "Not active in last 7 days"
                        if not last_seen:
                            stale.append(upstream)
                        access_display.append({
                            "Source": upstream,
                            "Type": r[3],
                            "Last Active": label,
                        })

                    if stale:
                        st.warning(
                            f"**Possible root cause:** {', '.join(stale)} "
                            f"{'has' if len(stale) == 1 else 'have'} not been active in the last 7 days. "
                            "This could be why your downstream report is broken or out of date."
                        )
                    else:
                        st.success("All source tables have been active recently — the issue is likely in the transformation logic, not the source data.")

                    st.dataframe(access_display, use_container_width=True, hide_index=True)

                except Exception as e:
                    st.warning(f"Could not fetch access history: {e}")

        # -- Recent errors --
        with st.spinner("Checking for recent errors..."):
            try:
                error_sql = """
                SELECT
                    query_id,
                    LEFT(query_text, 150)  AS query_snippet,
                    error_message,
                    start_time             AS error_time
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE execution_status = 'FAIL'
                  AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP)
                ORDER BY start_time DESC
                LIMIT 20
                """
                error_rows = session.sql(error_sql).collect()

                st.markdown("### Recent Query Errors (last 7 days)")
                if not error_rows:
                    st.success("No query errors in the last 7 days.")
                else:
                    error_data = [
                        {
                            "Time": r[3],
                            "Error": r[2],
                            "Query": r[1],
                            "Query ID": r[0],
                        }
                        for r in error_rows
                    ]
                    st.dataframe(error_data, use_container_width=True, hide_index=True)

            except Exception as e:
                st.warning(f"Could not fetch query errors: {e}")


# -- Footer --
st.divider()
st.caption("SnowGuard v0.3.1 | Built as a Snowflake Native App | Powered by Cortex AI")
