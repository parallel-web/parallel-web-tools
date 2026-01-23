-- =============================================================================
-- Parallel API Cleanup Script for Snowflake
-- =============================================================================
-- This script removes all Parallel integration objects from Snowflake.
--
-- WARNING: This will delete:
-- - All Parallel enrichment UDFs
-- - External access integration
-- - API key secret
-- - Network rule
-- - Roles
--
-- Prerequisites:
-- - ACCOUNTADMIN role or equivalent permissions
-- =============================================================================

USE DATABASE PARALLEL_INTEGRATION;
USE SCHEMA ENRICHMENT;

-- =============================================================================
-- Step 1: Drop UDFs
-- =============================================================================

DROP FUNCTION IF EXISTS PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(OBJECT, ARRAY);
DROP FUNCTION IF EXISTS PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(OBJECT, ARRAY, VARCHAR);
DROP FUNCTION IF EXISTS PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich_internal(OBJECT, ARRAY, VARCHAR, VARCHAR);

-- =============================================================================
-- Step 2: Drop External Access Integration
-- =============================================================================

DROP INTEGRATION IF EXISTS parallel_api_access_integration;

-- =============================================================================
-- Step 3: Drop Secret
-- =============================================================================

DROP SECRET IF EXISTS PARALLEL_INTEGRATION.ENRICHMENT.parallel_api_key;

-- =============================================================================
-- Step 4: Drop Network Rule
-- =============================================================================

DROP NETWORK RULE IF EXISTS PARALLEL_INTEGRATION.ENRICHMENT.parallel_api_network_rule;

-- =============================================================================
-- Step 5: Drop Roles
-- =============================================================================

DROP ROLE IF EXISTS PARALLEL_USER;
DROP ROLE IF EXISTS PARALLEL_DEVELOPER;

-- =============================================================================
-- Step 6: Optional - Drop Schema and Database
-- =============================================================================
-- Uncomment these lines to also remove the database and schema

-- DROP SCHEMA IF EXISTS PARALLEL_INTEGRATION.ENRICHMENT;
-- DROP DATABASE IF EXISTS PARALLEL_INTEGRATION;

SELECT 'Cleanup complete! All Parallel integration objects have been removed.' AS status;
