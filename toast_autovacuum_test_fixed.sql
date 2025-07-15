-- =================================================================
-- TOAST AUTOVACUUM TRUNCATION BUG TEST SCRIPT
-- =================================================================
-- This script tests whether TOAST tables inherit vacuum_truncate
-- settings from parent tables during AUTOVACUUM processing.
--
-- With debugging enabled, you should see LOG messages showing
-- the truncate decisions for both manual VACUUM and autovacuum.
-- =================================================================

-- Enable verbose autovacuum logging
-- Choose ONE of these approaches:

-- OPTION 1: System-wide (VERY VERBOSE - use with caution!)
-- WARNING: This will make PostgreSQL extremely verbose for ALL databases!
/*
ALTER SYSTEM SET log_autovacuum_min_duration = 0;        -- Log all autovacuum activity
ALTER SYSTEM SET log_min_messages = 'DEBUG1';            -- More detailed logging
ALTER SYSTEM SET log_line_prefix = '%t [%p] %q%u@%d ';   -- Better log format
ALTER SYSTEM SET log_checkpoints = off;                   -- Log checkpoints
ALTER SYSTEM SET log_statement = 'mod';                  -- Log modifications
SELECT pg_reload_conf();
*/

-- OPTION 2: Database-specific (RECOMMENDED)
ALTER DATABASE postgres SET log_autovacuum_min_duration = 0;
ALTER DATABASE postgres SET log_min_messages = 'INFO';     -- Less verbose than DEBUG1

-- Apply settings (will take effect for new connections)
-- For immediate effect, reconnect or run: SET log_min_messages = 'INFO';

-- Verify settings
SELECT name, setting FROM pg_settings WHERE name IN (
    'log_autovacuum_min_duration',
    'log_min_messages',
    'log_statement',
    'log_checkpoints'
);

-- Clean up any existing test table
DROP TABLE IF EXISTS toast_truncate_test;

-- 1. Create test table with VERY AGGRESSIVE autovacuum settings
CREATE TABLE toast_truncate_test (
    id SERIAL PRIMARY KEY,
    large_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
) WITH (
    -- Make autovacuum extremely aggressive for fast testing
    autovacuum_vacuum_threshold = 0,        -- Vacuum after just 5 dead tuples
    autovacuum_vacuum_scale_factor = 0.01,  -- Very low scale factor
    autovacuum_vacuum_cost_delay = 0,       -- No delay
    autovacuum_vacuum_cost_limit = 1000
);

ALTER TABLE toast_truncate_test SET (vacuum_truncate = false);

-- 2. Generate large JSONB data that will definitely use TOAST (>8KB each)
INSERT INTO toast_truncate_test (large_data)
SELECT jsonb_build_object(
    'id', i,
    'large_text', repeat('This is test data for TOAST storage testing. ', 10000),
    'more_data', repeat('Additional filler data to ensure TOAST usage. ', 10000),
    'timestamp', NOW(),
    'random_data', ARRAY[random()::text, random()::text, random()::text, random()::text]
)
FROM generate_series(1, 25000) i;

-- dead tuples
SELECT schemaname, relname, n_dead_tup, n_live_tup
FROM pg_stat_user_tables
WHERE relname LIKE 'pg_toast%';

DELETE FROM toast_truncate_test WHERE id > 100;


vacuum verbose toast_truncate_test;


-- 3. Verify TOAST table was created and check settings
SELECT
    'TABLE CREATED' as status,
    c.relname AS main_table,
    t.relname AS toast_table,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS main_table_size,
    pg_size_pretty(pg_total_relation_size(t.oid)) AS toast_table_size
FROM pg_class c
JOIN pg_class t ON c.reltoastrelid = t.oid
WHERE c.relname = 'toast_truncate_test';

-- 4. Check storage parameters - IMPORTANT: Note TOAST table has NO explicit settings
SELECT
    'STORAGE PARAMETERS' as info,
    c.relname AS table_name,
    c.reloptions AS table_options,
    CASE
        WHEN c.reloptions IS NULL THEN 'NO EXPLICIT SETTINGS - SHOULD INHERIT FROM PARENT'
        ELSE 'HAS EXPLICIT SETTINGS'
    END as inheritance_status
FROM pg_class c
WHERE c.relname = 'toast_truncate_test'
UNION ALL
SELECT
    'STORAGE PARAMETERS' as info,
    t.relname AS table_name,
    t.reloptions AS table_options,
    CASE
        WHEN t.reloptions IS NULL THEN 'NO EXPLICIT SETTINGS - SHOULD INHERIT FROM PARENT'
        ELSE 'HAS EXPLICIT SETTINGS'
    END as inheritance_status
FROM pg_class c
JOIN pg_class t ON c.reltoastrelid = t.oid
WHERE c.relname = 'toast_truncate_test';

-- 5. Create dead tuples to trigger autovacuum
DELETE FROM toast_truncate_test WHERE id > 1000;  -- Delete 90% of data

-- 6. Check table stats to see dead tuples
SELECT
    'AFTER DELETION' as phase,
    schemaname,
    relname,
    n_live_tup,
    n_dead_tup,
    last_vacuum,
    last_autovacuum,
    autovacuum_count
FROM pg_stat_user_tables
WHERE relname = 'toast_truncate_test';

-- 7. Get TOAST table name for monitoring
DO $$
DECLARE
    toast_table_name text;
BEGIN
    SELECT t.relname INTO toast_table_name
    FROM pg_class c
    JOIN pg_class t ON c.reltoastrelid = t.oid
    WHERE c.relname = 'toast_truncate_test';

    RAISE NOTICE '==================================================';
    RAISE NOTICE 'TOAST table name: %', toast_table_name;
    RAISE NOTICE 'Monitor logs for: VACUUM TRUNCATE DEBUG';
    RAISE NOTICE '==================================================';
END
$$;

-- 8. Force manual vacuum first (for comparison)
SELECT 'MANUAL VACUUM TEST - Should respect parent vacuum_truncate=false' as test_info;
VACUUM (VERBOSE) toast_truncate_test;

-- 9. Monitoring queries - run these in separate sessions
SELECT '=== MONITORING QUERIES ===' as info;
SELECT 'Run these queries to monitor autovacuum activity:' as instructions;

SELECT 'QUERY 1 - Check autovacuum stats:' as query_info;
SELECT 'SELECT schemaname, relname, last_autovacuum, autovacuum_count, n_dead_tup, n_live_tup FROM pg_stat_user_tables WHERE relname LIKE ''%toast%'' OR relname = ''toast_truncate_test'';' as query;

SELECT 'QUERY 2 - Check active autovacuum processes:' as query_info;
SELECT 'SELECT pid, query, state, query_start FROM pg_stat_activity WHERE query LIKE ''%autovacuum%'' AND state = ''active'';' as query;

SELECT 'QUERY 3 - Check table sizes:' as query_info;
SELECT 'SELECT c.relname, pg_size_pretty(pg_total_relation_size(c.oid)) as size FROM pg_class c WHERE c.relname = ''toast_truncate_test'' OR c.relname LIKE ''pg_toast_%'';' as query;

-- 10. Wait instructions
SELECT '=== AUTOVACUUM WAIT INSTRUCTIONS ===' as info;
SELECT 'Now wait for autovacuum to process the TOAST table...' as instruction;
SELECT 'Watch PostgreSQL logs for messages like:' as log_info;
SELECT '  - VACUUM TRUNCATE DEBUG: relation "pg_toast_XXXXX" ... autovacuum_worker=true' as pattern1;
SELECT '  - Should show: using global default vacuum_truncate=true (BUG!)' as pattern2;
SELECT '  - This proves TOAST table ignores parent vacuum_truncate=false' as pattern3;

-- 11. Create more dead tuples if needed
SELECT '=== CREATE MORE DEAD TUPLES (run if autovacuum is slow) ===' as more_dead_tuples;
SELECT 'INSERT INTO toast_truncate_test (large_data) SELECT jsonb_build_object(''test'', repeat(''data'', 2000)) FROM generate_series(1, 100);' as insert_more;
SELECT 'DELETE FROM toast_truncate_test WHERE id > 500;' as delete_more;

-- 12. Test the fix - set toast.vacuum_truncate explicitly
SELECT '=== TESTING THE FIX ===' as fix_test;
SELECT 'After observing the bug, run this to fix it:' as fix_instruction;
SELECT 'ALTER TABLE toast_truncate_test SET (toast.vacuum_truncate = false);' as fix_command;

-- 13. Show expected behavior
SELECT '=== EXPECTED BEHAVIOR ===' as expected;
SELECT 'BEFORE setting toast.vacuum_truncate = false:' as before_fix;
SELECT '  - Manual VACUUM: Should respect parent vacuum_truncate=false (NO truncation)' as manual_behavior;
SELECT '  - Autovacuum: Should ignore parent setting and truncate anyway (BUG!)' as autovacuum_bug;
SELECT 'AFTER setting toast.vacuum_truncate = false:' as after_fix;
SELECT '  - Both manual VACUUM and autovacuum should respect the setting (NO truncation)' as fixed_behavior;

-- 14. Log monitoring command
SELECT '=== LOG MONITORING ===' as log_monitoring;
SELECT 'Monitor PostgreSQL logs with:' as log_command_info;
SELECT 'tail -f /path/to/postgresql.log | grep "VACUUM TRUNCATE DEBUG"' as log_command;

-- 15. CLEANUP SECTION (run after testing)
SELECT '=== CLEANUP COMMANDS ===' as cleanup_info;
SELECT 'After testing, reset system settings with:' as reset_info;

/*
-- CLEANUP OPTION 1: If you used system-wide settings
ALTER SYSTEM RESET log_autovacuum_min_duration;
ALTER SYSTEM RESET log_min_messages;
ALTER SYSTEM RESET log_line_prefix;
ALTER SYSTEM RESET log_checkpoints;
ALTER SYSTEM RESET log_statement;
SELECT pg_reload_conf();

-- CLEANUP OPTION 2: If you used database-specific settings
ALTER DATABASE postgres RESET log_autovacuum_min_duration;
ALTER DATABASE postgres RESET log_min_messages;

-- Drop test table
DROP TABLE toast_truncate_test;
*/

SELECT '==================================================================' as separator;
SELECT 'TEST SETUP COMPLETE!' as status;
SELECT 'The table has aggressive autovacuum settings and many dead tuples.' as setup_info;
SELECT 'Autovacuum should run within seconds/minutes.' as timing_info;
SELECT 'Watch the logs for VACUUM TRUNCATE DEBUG messages!' as final_instruction;
SELECT 'You should now see MUCH more detailed autovacuum logs!' as verbose_info;
SELECT '==================================================================' as separator;
