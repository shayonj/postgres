--
-- MISC_SANITY
-- Sanity checks for common errors in making system tables that don't fit
-- comfortably into either opr_sanity or type_sanity.
--
-- Every test failure in this file should be closely inspected.
-- The description of the failing test should be read carefully before
-- adjusting the expected output.  In most cases, the queries should
-- not find *any* matching entries.
--
-- NB: run this test early, because some later tests create bogus entries.


-- **************** pg_depend ****************

-- Look for illegal values in pg_depend fields.

SELECT *
FROM pg_depend as d1
WHERE refclassid = 0 OR refobjid = 0 OR
      classid = 0 OR objid = 0 OR
      deptype NOT IN ('a', 'e', 'i', 'n', 'x', 'P', 'S');


-- **************** pg_shdepend ****************

-- Look for illegal values in pg_shdepend fields.

SELECT *
FROM pg_shdepend as d1
WHERE refclassid = 0 OR refobjid = 0 OR
      classid = 0 OR objid = 0 OR
      deptype NOT IN ('a', 'i', 'o', 'r', 't');


-- **************** pg_class ****************

-- Look for system tables with varlena columns but no toast table. All
-- system tables with toastable columns should have toast tables, with
-- the following exceptions:
-- 1. pg_class and pg_attribute, due to fear of recursive dependencies as
-- toast tables depend on them.
-- 2. pg_largeobject and pg_largeobject_metadata.  Large object catalogs
-- and toast tables are mutually exclusive and large object data is handled
-- as user data by pg_upgrade, which would cause failures.
-- 3. pg_authid, since its toast table cannot be accessed when it would be
-- needed, i.e., during authentication before we've selected a database.
-- 4. pg_replication_origin, since we want to be able to access that catalog
-- without setting up a snapshot.  To make that safe, it needs to not have a
-- toast table, since toasted data cannot be fetched without a snapshot.  As of
-- this writing, its only varlena column is roname, which we limit to 512 bytes
-- to avoid needing out-of-line storage.

SELECT relname, attname, atttypid::regtype
FROM pg_class c JOIN pg_attribute a ON c.oid = attrelid
WHERE c.oid < 16384 AND
      reltoastrelid = 0 AND
      relkind = 'r' AND
      attstorage != 'p'
ORDER BY 1, 2;


-- system catalogs without primary keys
--
-- Current exceptions:
-- * pg_depend, pg_shdepend don't have a unique key
SELECT relname
FROM pg_class
WHERE relnamespace = 'pg_catalog'::regnamespace AND relkind = 'r'
      AND pg_class.oid NOT IN (SELECT indrelid FROM pg_index WHERE indisprimary)
ORDER BY 1;


-- system catalog unique indexes not wrapped in a constraint
-- (There should be none.)
SELECT relname
FROM pg_class c JOIN pg_index i ON c.oid = i.indexrelid
WHERE relnamespace = 'pg_catalog'::regnamespace AND relkind = 'i'
      AND i.indisunique
      AND c.oid NOT IN (SELECT conindid FROM pg_constraint)
ORDER BY 1;
