-- ============================================================================
-- SnowGuard: Data Quality Monitor — Setup Script
-- ============================================================================
-- This script runs when the Native App is installed (or upgraded) in a
-- consumer's Snowflake account. It creates all the schemas, stored procedures,
-- application roles, and Streamlit references the app needs.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. APPLICATION ROLE
-- ----------------------------------------------------------------------------
CREATE APPLICATION ROLE IF NOT EXISTS app_public;

-- ----------------------------------------------------------------------------
-- 2. RESULTS SCHEMA — must be created BEFORE core (procedures insert here)
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS results;
GRANT USAGE ON SCHEMA results TO APPLICATION ROLE app_public;

CREATE TABLE IF NOT EXISTS results.quality_checks (
    check_id NUMBER AUTOINCREMENT,
    check_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    database_name VARCHAR,
    schema_name VARCHAR,
    table_name VARCHAR,
    check_type VARCHAR,
    check_result VARCHAR,
    details VARIANT,
    PRIMARY KEY (check_id)
);

GRANT SELECT ON TABLE results.quality_checks TO APPLICATION ROLE app_public;

-- ----------------------------------------------------------------------------
-- 3. CORE SCHEMA — data quality logic
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS core;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_public;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- PROCEDURE: Null Percentage Check
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CREATE OR REPLACE PROCEDURE core.check_null_percentage(
    db_name VARCHAR, schema_name VARCHAR, tbl_name VARCHAR
)
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    DECLARE
        full_table VARCHAR;
        overall_result VARCHAR DEFAULT 'pass';
        total_rows NUMBER DEFAULT 0;
        column_results ARRAY DEFAULT ARRAY_CONSTRUCT();
        info_query VARCHAR;
        count_query VARCHAR;
    BEGIN
        full_table := :db_name || '.' || :schema_name || '.' || :tbl_name;

        -- Get total row count
        count_query := 'SELECT COUNT(*) as cnt FROM ' || :full_table;
        LET rs RESULTSET := (EXECUTE IMMEDIATE :count_query);
        LET c1 CURSOR FOR rs;
        FOR row_var IN c1 DO
            total_rows := row_var.CNT;
        END FOR;

        -- Get columns from INFORMATION_SCHEMA
        info_query := 'SELECT COLUMN_NAME FROM ' || :db_name ||
            '.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''' ||
            UPPER(:schema_name) || ''' AND TABLE_NAME = ''' ||
            UPPER(:tbl_name) || ''' ORDER BY ORDINAL_POSITION';
        LET col_rs RESULTSET := (EXECUTE IMMEDIATE :info_query);

        LET c2 CURSOR FOR col_rs;
        FOR col_row IN c2 DO
            LET col_name VARCHAR := col_row.COLUMN_NAME;
            LET null_count NUMBER := 0;
            LET null_query VARCHAR := 'SELECT COUNT(*) as cnt FROM ' || :full_table ||
                ' WHERE "' || :col_name || '" IS NULL';
            LET null_rs RESULTSET := (EXECUTE IMMEDIATE :null_query);
            LET c3 CURSOR FOR null_rs;
            FOR null_row IN c3 DO
                null_count := null_row.CNT;
            END FOR;

            LET null_pct FLOAT := 0;
            IF (:total_rows > 0) THEN
                null_pct := ROUND(:null_count / :total_rows * 100, 2);
            END IF;

            LET col_status VARCHAR := 'pass';
            IF (:null_pct > 30) THEN
                col_status := 'fail';
                overall_result := 'fail';
            ELSEIF (:null_pct > 10) THEN
                col_status := 'warn';
                IF (:overall_result != 'fail') THEN
                    overall_result := 'warn';
                END IF;
            END IF;

            column_results := ARRAY_APPEND(:column_results, OBJECT_CONSTRUCT(
                'column_name', :col_name,
                'null_count', :null_count,
                'total_rows', :total_rows,
                'null_pct', :null_pct,
                'status', :col_status
            ));
        END FOR;

        -- Build the return object
        LET return_obj VARIANT := OBJECT_CONSTRUCT(
            'check_type', 'null_percentage',
            'table', :full_table,
            'result', :overall_result,
            'total_rows', :total_rows,
            'columns', :column_results
        );

        -- Insert result into history using SELECT (VALUES doesn't support VARIANT expressions)
        INSERT INTO results.quality_checks (database_name, schema_name, table_name, check_type, check_result, details)
        SELECT :db_name, :schema_name, :tbl_name, 'null_percentage', :overall_result, :return_obj;

        RETURN :return_obj;
    END;

GRANT USAGE ON PROCEDURE core.check_null_percentage(VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- PROCEDURE: Row Count Check
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CREATE OR REPLACE PROCEDURE core.check_row_count(
    db_name VARCHAR, schema_name VARCHAR, tbl_name VARCHAR
)
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    DECLARE
        full_table VARCHAR;
        current_count NUMBER DEFAULT 0;
        previous_count NUMBER DEFAULT NULL;
        pct_change FLOAT DEFAULT 0;
        result_status VARCHAR DEFAULT 'pass';
        count_query VARCHAR;
    BEGIN
        full_table := :db_name || '.' || :schema_name || '.' || :tbl_name;

        -- Get current count
        count_query := 'SELECT COUNT(*) as cnt FROM ' || :full_table;
        LET count_rs RESULTSET := (EXECUTE IMMEDIATE :count_query);
        LET c1 CURSOR FOR count_rs;
        FOR row_var IN c1 DO
            current_count := row_var.CNT;
        END FOR;

        -- Get previous count from history
        LET prev_rs RESULTSET := (
            SELECT details:row_count::NUMBER as prev_count
            FROM results.quality_checks
            WHERE database_name = :db_name
              AND schema_name = :schema_name
              AND table_name = :tbl_name
              AND check_type = 'row_count'
            ORDER BY check_timestamp DESC
            LIMIT 1
        );
        LET c2 CURSOR FOR prev_rs;
        FOR prev_row IN c2 DO
            previous_count := prev_row.PREV_COUNT;
        END FOR;

        -- Calculate change
        IF (:previous_count IS NOT NULL AND :previous_count > 0) THEN
            pct_change := ROUND(ABS(:current_count - :previous_count) / :previous_count * 100, 2);
            IF (:pct_change > 80) THEN
                result_status := 'fail';
            ELSEIF (:pct_change > 50) THEN
                result_status := 'warn';
            END IF;
        END IF;

        -- Build the return object
        LET return_obj VARIANT := OBJECT_CONSTRUCT(
            'check_type', 'row_count',
            'table', :full_table,
            'result', :result_status,
            'row_count', :current_count,
            'previous_count', :previous_count,
            'pct_change', :pct_change
        );

        -- Insert result into history
        INSERT INTO results.quality_checks (database_name, schema_name, table_name, check_type, check_result, details)
        SELECT :db_name, :schema_name, :tbl_name, 'row_count', :result_status, :return_obj;

        RETURN :return_obj;
    END;

GRANT USAGE ON PROCEDURE core.check_row_count(VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- PROCEDURE: Schema Drift Check
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CREATE OR REPLACE PROCEDURE core.check_schema_drift(
    db_name VARCHAR, schema_name VARCHAR, tbl_name VARCHAR
)
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    DECLARE
        full_table VARCHAR;
        current_schema VARIANT;
        previous_schema VARIANT DEFAULT NULL;
        result_status VARCHAR DEFAULT 'pass';
        drift_details VARIANT DEFAULT NULL;
        schema_query VARCHAR;
    BEGIN
        full_table := :db_name || '.' || :schema_name || '.' || :tbl_name;

        -- Get current schema via dynamic SQL
        schema_query := 'SELECT ARRAY_AGG(OBJECT_CONSTRUCT(' ||
            '''column_name'', COLUMN_NAME, ' ||
            '''data_type'', DATA_TYPE, ' ||
            '''ordinal_position'', ORDINAL_POSITION, ' ||
            '''is_nullable'', IS_NULLABLE' ||
            ')) as schema_info FROM ' || :db_name ||
            '.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''' ||
            UPPER(:schema_name) || ''' AND TABLE_NAME = ''' ||
            UPPER(:tbl_name) || '''';
        LET schema_rs RESULTSET := (EXECUTE IMMEDIATE :schema_query);
        LET c1 CURSOR FOR schema_rs;
        FOR row_var IN c1 DO
            current_schema := row_var.SCHEMA_INFO;
        END FOR;

        -- Get previous schema from history
        LET prev_rs RESULTSET := (
            SELECT details:schema_snapshot as prev_schema
            FROM results.quality_checks
            WHERE database_name = :db_name
              AND schema_name = :schema_name
              AND table_name = :tbl_name
              AND check_type = 'schema_drift'
            ORDER BY check_timestamp DESC
            LIMIT 1
        );
        LET c2 CURSOR FOR prev_rs;
        FOR prev_row IN c2 DO
            previous_schema := prev_row.PREV_SCHEMA;
        END FOR;

        -- Compare schemas if we have a previous snapshot
        IF (:previous_schema IS NOT NULL) THEN
            LET current_cols ARRAY := ARRAY_CONSTRUCT();
            LET previous_cols ARRAY := ARRAY_CONSTRUCT();

            LET i NUMBER := 0;
            WHILE (:i < ARRAY_SIZE(:current_schema)) DO
                current_cols := ARRAY_APPEND(:current_cols, :current_schema[:i]:column_name::VARCHAR);
                i := :i + 1;
            END WHILE;

            LET j NUMBER := 0;
            WHILE (:j < ARRAY_SIZE(:previous_schema)) DO
                previous_cols := ARRAY_APPEND(:previous_cols, :previous_schema[:j]:column_name::VARCHAR);
                j := :j + 1;
            END WHILE;

            LET k NUMBER := 0;
            LET added ARRAY := ARRAY_CONSTRUCT();
            WHILE (:k < ARRAY_SIZE(:current_cols)) DO
                IF (NOT ARRAY_CONTAINS(:current_cols[:k]::VARIANT, :previous_cols)) THEN
                    added := ARRAY_APPEND(:added, :current_cols[:k]);
                    result_status := 'warn';
                END IF;
                k := :k + 1;
            END WHILE;

            LET m NUMBER := 0;
            LET removed ARRAY := ARRAY_CONSTRUCT();
            WHILE (:m < ARRAY_SIZE(:previous_cols)) DO
                IF (NOT ARRAY_CONTAINS(:previous_cols[:m]::VARIANT, :current_cols)) THEN
                    removed := ARRAY_APPEND(:removed, :previous_cols[:m]);
                    result_status := 'fail';
                END IF;
                m := :m + 1;
            END WHILE;

            drift_details := OBJECT_CONSTRUCT(
                'columns_added', :added,
                'columns_removed', :removed,
                'current_column_count', ARRAY_SIZE(:current_cols),
                'previous_column_count', ARRAY_SIZE(:previous_cols)
            );
        ELSE
            drift_details := OBJECT_CONSTRUCT(
                'message', 'First schema snapshot captured. Run again later to detect drift.',
                'column_count', ARRAY_SIZE(:current_schema)
            );
        END IF;

        -- Build the return object
        LET return_obj VARIANT := OBJECT_CONSTRUCT(
            'check_type', 'schema_drift',
            'table', :full_table,
            'result', :result_status,
            'current_schema', :current_schema,
            'drift', :drift_details
        );

        -- Insert result into history
        INSERT INTO results.quality_checks (database_name, schema_name, table_name, check_type, check_result, details)
        SELECT :db_name, :schema_name, :tbl_name, 'schema_drift', :result_status, :return_obj;

        RETURN :return_obj;
    END;

GRANT USAGE ON PROCEDURE core.check_schema_drift(VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- PROCEDURE: Value Distribution Check
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CREATE OR REPLACE PROCEDURE core.check_value_distribution(
    db_name VARCHAR, schema_name VARCHAR, tbl_name VARCHAR
)
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    DECLARE
        full_table VARCHAR;
        result_status VARCHAR DEFAULT 'pass';
        distribution_results ARRAY DEFAULT ARRAY_CONSTRUCT();
        num_query VARCHAR;
        str_query VARCHAR;
    BEGIN
        full_table := :db_name || '.' || :schema_name || '.' || :tbl_name;

        -- Get numeric columns
        num_query := 'SELECT COLUMN_NAME, DATA_TYPE FROM ' || :db_name ||
            '.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''' ||
            UPPER(:schema_name) || ''' AND TABLE_NAME = ''' ||
            UPPER(:tbl_name) || ''' AND DATA_TYPE IN (''NUMBER'', ''FLOAT'', ''DECIMAL'')';
        LET num_rs RESULTSET := (EXECUTE IMMEDIATE :num_query);
        LET c1 CURSOR FOR num_rs;
        FOR num_col IN c1 DO
            LET col_name VARCHAR := num_col.COLUMN_NAME;
            LET stats_query VARCHAR := 'SELECT MIN("' || :col_name || '") as min_val, ' ||
                'MAX("' || :col_name || '") as max_val, ' ||
                'ROUND(AVG("' || :col_name || '"), 2) as avg_val, ' ||
                'ROUND(STDDEV("' || :col_name || '"), 2) as stddev_val, ' ||
                'COUNT(DISTINCT "' || :col_name || '") as distinct_count ' ||
                'FROM ' || :full_table || ' WHERE "' || :col_name || '" IS NOT NULL';
            LET stats_rs RESULTSET := (EXECUTE IMMEDIATE :stats_query);
            LET c2 CURSOR FOR stats_rs;
            FOR stats_row IN c2 DO
                LET range_ratio FLOAT := 0;
                IF (stats_row.AVG_VAL IS NOT NULL AND stats_row.AVG_VAL != 0 AND stats_row.STDDEV_VAL IS NOT NULL) THEN
                    range_ratio := stats_row.STDDEV_VAL / ABS(stats_row.AVG_VAL);
                    IF (:range_ratio > 3) THEN
                        result_status := 'warn';
                    END IF;
                END IF;
                distribution_results := ARRAY_APPEND(:distribution_results, OBJECT_CONSTRUCT(
                    'column_name', :col_name,
                    'data_type', 'NUMERIC',
                    'min', stats_row.MIN_VAL,
                    'max', stats_row.MAX_VAL,
                    'avg', stats_row.AVG_VAL,
                    'stddev', stats_row.STDDEV_VAL,
                    'distinct_count', stats_row.DISTINCT_COUNT,
                    'coefficient_of_variation', ROUND(:range_ratio, 2)
                ));
            END FOR;
        END FOR;

        -- Get varchar columns
        str_query := 'SELECT COLUMN_NAME FROM ' || :db_name ||
            '.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''' ||
            UPPER(:schema_name) || ''' AND TABLE_NAME = ''' ||
            UPPER(:tbl_name) || ''' AND DATA_TYPE IN (''TEXT'', ''VARCHAR'')';
        LET str_rs RESULTSET := (EXECUTE IMMEDIATE :str_query);
        LET c3 CURSOR FOR str_rs;
        FOR str_col IN c3 DO
            LET str_col_name VARCHAR := str_col.COLUMN_NAME;
            LET top_query VARCHAR := 'SELECT ARRAY_AGG(OBJECT_CONSTRUCT(''value'', val, ''count'', cnt)) as top_vals FROM (' ||
                'SELECT "' || :str_col_name || '" as val, COUNT(*) as cnt ' ||
                'FROM ' || :full_table || ' WHERE "' || :str_col_name || '" IS NOT NULL ' ||
                'GROUP BY "' || :str_col_name || '" ORDER BY cnt DESC LIMIT 5)';
            LET top_rs RESULTSET := (EXECUTE IMMEDIATE :top_query);
            LET c4 CURSOR FOR top_rs;
            FOR top_row IN c4 DO
                LET distinct_query VARCHAR := 'SELECT COUNT(DISTINCT "' || :str_col_name || '") as cnt FROM ' || :full_table;
                LET dist_rs RESULTSET := (EXECUTE IMMEDIATE :distinct_query);
                LET distinct_count NUMBER := 0;
                LET c5 CURSOR FOR dist_rs;
                FOR dist_row IN c5 DO
                    distinct_count := dist_row.CNT;
                END FOR;
                distribution_results := ARRAY_APPEND(:distribution_results, OBJECT_CONSTRUCT(
                    'column_name', :str_col_name,
                    'data_type', 'VARCHAR',
                    'distinct_count', :distinct_count,
                    'top_values', top_row.TOP_VALS
                ));
            END FOR;
        END FOR;

        -- Build the return object
        LET return_obj VARIANT := OBJECT_CONSTRUCT(
            'check_type', 'value_distribution',
            'table', :full_table,
            'result', :result_status,
            'distributions', :distribution_results
        );

        -- Insert result into history
        INSERT INTO results.quality_checks (database_name, schema_name, table_name, check_type, check_result, details)
        SELECT :db_name, :schema_name, :tbl_name, 'value_distribution', :result_status, :return_obj;

        RETURN :return_obj;
    END;

GRANT USAGE ON PROCEDURE core.check_value_distribution(VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- PROCEDURE: Run All Checks
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CREATE OR REPLACE PROCEDURE core.run_all_checks(
    db_name VARCHAR, schema_name VARCHAR, tbl_name VARCHAR
)
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    DECLARE
        null_result VARIANT;
        row_result VARIANT;
        schema_result VARIANT;
        dist_result VARIANT;
    BEGIN
        CALL core.check_null_percentage(:db_name, :schema_name, :tbl_name) INTO :null_result;
        CALL core.check_row_count(:db_name, :schema_name, :tbl_name) INTO :row_result;
        CALL core.check_schema_drift(:db_name, :schema_name, :tbl_name) INTO :schema_result;
        CALL core.check_value_distribution(:db_name, :schema_name, :tbl_name) INTO :dist_result;

        RETURN OBJECT_CONSTRUCT(
            'table', :db_name || '.' || :schema_name || '.' || :tbl_name,
            'null_percentage', :null_result,
            'row_count', :row_result,
            'schema_drift', :schema_result,
            'value_distribution', :dist_result
        );
    END;

GRANT USAGE ON PROCEDURE core.run_all_checks(VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- PROCEDURE: Get Check History
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CREATE OR REPLACE PROCEDURE core.get_check_history(
    db_name VARCHAR, schema_name VARCHAR, tbl_name VARCHAR
)
    RETURNS TABLE(
        check_id NUMBER, check_timestamp TIMESTAMP_NTZ, check_type VARCHAR,
        check_result VARCHAR, details VARIANT
    )
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    BEGIN
        LET rs RESULTSET := (
            SELECT check_id, check_timestamp, check_type, check_result, details
            FROM results.quality_checks
            WHERE database_name = :db_name
              AND schema_name = :schema_name
              AND table_name = :tbl_name
            ORDER BY check_timestamp DESC
            LIMIT 50
        );
        RETURN TABLE(rs);
    END;

GRANT USAGE ON PROCEDURE core.get_check_history(VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- PROCEDURE: Generate AI Summary (Cortex LLM)
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CREATE OR REPLACE PROCEDURE core.generate_ai_summary(check_results VARIANT)
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    DECLARE
        prompt VARCHAR;
        ai_response VARCHAR;
        table_name VARCHAR;
    BEGIN
        -- Extract table name from results
        table_name := :check_results:table::VARCHAR;
        
        -- Build the prompt for the LLM
        prompt := 'You are a data quality analyst. Analyze these data quality check results and provide a brief, actionable summary in 2-3 sentences. Focus on any warnings or failures and what they might mean for data users. Be specific about column names and values.

Check Results for table ' || :table_name || ':
' || :check_results::VARCHAR || '

Provide a clear, concise summary:';

        -- Call Cortex LLM (using snowflake-arctic which is widely available)
        BEGIN
            SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', :prompt) INTO :ai_response;
        EXCEPTION
            WHEN OTHER THEN
                ai_response := 'AI summary unavailable. Cortex LLM may not be enabled in this region.';
        END;
        
        RETURN :ai_response;
    END;

GRANT USAGE ON PROCEDURE core.generate_ai_summary(VARIANT) TO APPLICATION ROLE app_public;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- PROCEDURE: Run All Checks with AI Summary
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CREATE OR REPLACE PROCEDURE core.run_all_checks_with_summary(
    db_name VARCHAR, schema_name VARCHAR, tbl_name VARCHAR
)
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    DECLARE
        check_results VARIANT;
        ai_summary VARCHAR;
    BEGIN
        -- Run all checks
        CALL core.run_all_checks(:db_name, :schema_name, :tbl_name) INTO :check_results;
        
        -- Generate AI summary
        CALL core.generate_ai_summary(:check_results) INTO :ai_summary;
        
        -- Add AI summary to results
        RETURN OBJECT_INSERT(:check_results, 'ai_summary', :ai_summary);
    END;

GRANT USAGE ON PROCEDURE core.run_all_checks_with_summary(VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;

-- ----------------------------------------------------------------------------
-- 4. CONFIG SCHEMA — Scheduled monitoring configuration
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS config;
GRANT USAGE ON SCHEMA config TO APPLICATION ROLE app_public;

CREATE TABLE IF NOT EXISTS config.monitored_tables (
    id NUMBER AUTOINCREMENT,
    database_name VARCHAR NOT NULL,
    schema_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    check_frequency VARCHAR DEFAULT 'HOURLY',  -- HOURLY, DAILY, WEEKLY
    enabled BOOLEAN DEFAULT TRUE,
    last_check_time TIMESTAMP_NTZ,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (id),
    UNIQUE (database_name, schema_name, table_name)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE config.monitored_tables TO APPLICATION ROLE app_public;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- PROCEDURE: Add Table to Monitoring
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CREATE OR REPLACE PROCEDURE core.add_monitored_table(
    db_name VARCHAR, schema_name VARCHAR, tbl_name VARCHAR, frequency VARCHAR
)
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    BEGIN
        INSERT INTO config.monitored_tables (database_name, schema_name, table_name, check_frequency)
        VALUES (:db_name, :schema_name, :tbl_name, :frequency);
        RETURN 'Table ' || :db_name || '.' || :schema_name || '.' || :tbl_name || ' added to monitoring.';
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'Table already being monitored or error: ' || SQLERRM;
    END;

GRANT USAGE ON PROCEDURE core.add_monitored_table(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- PROCEDURE: Remove Table from Monitoring
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CREATE OR REPLACE PROCEDURE core.remove_monitored_table(
    db_name VARCHAR, schema_name VARCHAR, tbl_name VARCHAR
)
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    BEGIN
        DELETE FROM config.monitored_tables
        WHERE database_name = :db_name
          AND schema_name = :schema_name
          AND table_name = :tbl_name;
        RETURN 'Table ' || :db_name || '.' || :schema_name || '.' || :tbl_name || ' removed from monitoring.';
    END;

GRANT USAGE ON PROCEDURE core.remove_monitored_table(VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- PROCEDURE: Toggle Monitoring
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CREATE OR REPLACE PROCEDURE core.toggle_monitoring(
    db_name VARCHAR, schema_name VARCHAR, tbl_name VARCHAR, is_enabled BOOLEAN
)
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    BEGIN
        UPDATE config.monitored_tables
        SET enabled = :is_enabled
        WHERE database_name = :db_name
          AND schema_name = :schema_name
          AND table_name = :tbl_name;
        RETURN 'Monitoring ' || IFF(:is_enabled, 'enabled', 'disabled') || ' for ' || 
               :db_name || '.' || :schema_name || '.' || :tbl_name;
    END;

GRANT USAGE ON PROCEDURE core.toggle_monitoring(VARCHAR, VARCHAR, VARCHAR, BOOLEAN) TO APPLICATION ROLE app_public;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- PROCEDURE: Run Scheduled Checks (called by Task)
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CREATE OR REPLACE PROCEDURE core.run_scheduled_checks()
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    DECLARE
        tables_checked NUMBER DEFAULT 0;
        check_result VARIANT;
        results ARRAY DEFAULT ARRAY_CONSTRUCT();
    BEGIN
        -- Get all enabled tables due for checking
        LET tables_rs RESULTSET := (
            SELECT database_name, schema_name, table_name, check_frequency
            FROM config.monitored_tables
            WHERE enabled = TRUE
        );
        
        LET c1 CURSOR FOR tables_rs;
        FOR tbl IN c1 DO
            LET tbl_db VARCHAR := tbl.DATABASE_NAME;
            LET tbl_sch VARCHAR := tbl.SCHEMA_NAME;
            LET tbl_tbl VARCHAR := tbl.TABLE_NAME;
            
            -- Run checks for this table
            BEGIN
                CALL core.run_all_checks(:tbl_db, :tbl_sch, :tbl_tbl) 
                    INTO :check_result;
                
                -- Update last check time
                UPDATE config.monitored_tables
                SET last_check_time = CURRENT_TIMESTAMP()
                WHERE database_name = :tbl_db
                  AND schema_name = :tbl_sch
                  AND table_name = :tbl_tbl;
                
                results := ARRAY_APPEND(:results, OBJECT_CONSTRUCT(
                    'table', :tbl_db || '.' || :tbl_sch || '.' || :tbl_tbl,
                    'status', 'success'
                ));
                tables_checked := :tables_checked + 1;
            EXCEPTION
                WHEN OTHER THEN
                    results := ARRAY_APPEND(:results, OBJECT_CONSTRUCT(
                        'table', :tbl_db || '.' || :tbl_sch || '.' || :tbl_tbl,
                        'status', 'error',
                        'error', SQLERRM
                    ));
            END;
        END FOR;
        
        RETURN OBJECT_CONSTRUCT(
            'run_timestamp', CURRENT_TIMESTAMP(),
            'tables_checked', :tables_checked,
            'results', :results
        );
    END;

GRANT USAGE ON PROCEDURE core.run_scheduled_checks() TO APPLICATION ROLE app_public;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- PROCEDURE: Get Monitored Tables
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CREATE OR REPLACE PROCEDURE core.get_monitored_tables()
    RETURNS TABLE(
        id NUMBER, database_name VARCHAR, schema_name VARCHAR, table_name VARCHAR,
        check_frequency VARCHAR, enabled BOOLEAN, last_check_time TIMESTAMP_NTZ
    )
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    BEGIN
        LET rs RESULTSET := (
            SELECT id, database_name, schema_name, table_name, 
                   check_frequency, enabled, last_check_time
            FROM config.monitored_tables
            ORDER BY created_at DESC
        );
        RETURN TABLE(rs);
    END;

GRANT USAGE ON PROCEDURE core.get_monitored_tables() TO APPLICATION ROLE app_public;

-- ----------------------------------------------------------------------------
-- 5. UI SCHEMA — Streamlit dashboard reference
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS ui;
GRANT USAGE ON SCHEMA ui TO APPLICATION ROLE app_public;

CREATE OR REPLACE STREAMLIT ui.snowguard_dashboard
    FROM '/'
    MAIN_FILE = 'streamlit_app.py';

GRANT USAGE ON STREAMLIT ui.snowguard_dashboard TO APPLICATION ROLE app_public;
