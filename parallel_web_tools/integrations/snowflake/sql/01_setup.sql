-- =============================================================================
-- Parallel API External Access Setup for Snowflake
-- =============================================================================
-- This script sets up the network infrastructure required to call the Parallel
-- API from Snowflake UDFs.
--
-- Prerequisites:
-- - ACCOUNTADMIN role or equivalent permissions
-- - Parallel API key from https://platform.parallel.ai
--
-- Configuration:
-- Replace 'YOUR_PARALLEL_API_KEY' with your actual API key
--
-- Usage:
-- Run this script once per Snowflake account to set up the integration.
-- =============================================================================

-- Create database and schema for Parallel integration
CREATE DATABASE IF NOT EXISTS PARALLEL_INTEGRATION;
CREATE SCHEMA IF NOT EXISTS PARALLEL_INTEGRATION.ENRICHMENT;

USE DATABASE PARALLEL_INTEGRATION;
USE SCHEMA ENRICHMENT;

-- =============================================================================
-- Step 1: Create Network Rule
-- =============================================================================
-- Allows HTTPS egress traffic to the Parallel API endpoint

CREATE OR REPLACE NETWORK RULE parallel_api_network_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('api.parallel.ai:443')
    COMMENT = 'Network rule for Parallel API access';

-- =============================================================================
-- Step 2: Create Secret
-- =============================================================================
-- Securely stores the Parallel API key
-- IMPORTANT: Replace 'YOUR_PARALLEL_API_KEY' with your actual API key

CREATE OR REPLACE SECRET parallel_api_key
    TYPE = GENERIC_STRING
    SECRET_STRING = 'YOUR_PARALLEL_API_KEY'
    COMMENT = 'Parallel API key for authentication';

-- =============================================================================
-- Step 3: Create External Access Integration
-- =============================================================================
-- Combines the network rule and secret to enable UDFs to make external API calls

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION parallel_api_access_integration
    ALLOWED_NETWORK_RULES = (parallel_api_network_rule)
    ALLOWED_AUTHENTICATION_SECRETS = (parallel_api_key)
    ENABLED = TRUE
    COMMENT = 'External access integration for Parallel API';

-- =============================================================================
-- Step 4: Grant PyPI Repository Access
-- =============================================================================
-- Required for UDFs to use parallel-web-tools package from PyPI

GRANT DATABASE ROLE SNOWFLAKE.PYPI_REPOSITORY_USER TO ROLE ACCOUNTADMIN;

-- =============================================================================
-- Step 5: Create Roles
-- =============================================================================
-- PARALLEL_DEVELOPER: Can create and modify UDFs
-- PARALLEL_USER: Can execute UDFs

CREATE ROLE IF NOT EXISTS PARALLEL_DEVELOPER;
CREATE ROLE IF NOT EXISTS PARALLEL_USER;

-- Grant PyPI access to developer role
GRANT DATABASE ROLE SNOWFLAKE.PYPI_REPOSITORY_USER TO ROLE PARALLEL_DEVELOPER;

-- Grant permissions to PARALLEL_DEVELOPER
GRANT USAGE ON DATABASE PARALLEL_INTEGRATION TO ROLE PARALLEL_DEVELOPER;
GRANT USAGE ON SCHEMA PARALLEL_INTEGRATION.ENRICHMENT TO ROLE PARALLEL_DEVELOPER;
GRANT CREATE FUNCTION ON SCHEMA PARALLEL_INTEGRATION.ENRICHMENT TO ROLE PARALLEL_DEVELOPER;
GRANT USAGE ON INTEGRATION parallel_api_access_integration TO ROLE PARALLEL_DEVELOPER;
GRANT READ ON SECRET parallel_api_key TO ROLE PARALLEL_DEVELOPER;

-- Grant permissions to PARALLEL_USER
GRANT USAGE ON DATABASE PARALLEL_INTEGRATION TO ROLE PARALLEL_USER;
GRANT USAGE ON SCHEMA PARALLEL_INTEGRATION.ENRICHMENT TO ROLE PARALLEL_USER;
GRANT USAGE ON FUNCTION PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(OBJECT, ARRAY) TO ROLE PARALLEL_USER;
GRANT USAGE ON FUNCTION PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(OBJECT, ARRAY, VARCHAR) TO ROLE PARALLEL_USER;

-- =============================================================================
-- Verification
-- =============================================================================
-- Run these queries to verify the setup

-- Check network rule
SHOW NETWORK RULES LIKE 'parallel_api_network_rule';

-- Check secret exists (value is hidden)
SHOW SECRETS LIKE 'parallel_api_key';

-- Check integration
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'parallel_api_access_integration';

-- Check roles
SHOW ROLES LIKE 'PARALLEL_%';

SELECT 'Setup complete! Run 02_create_udf.sql next.' AS status;
