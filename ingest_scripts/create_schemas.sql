-- create raw schema which is the landing zone which reflects the source database
DROP SCHEMA IF EXISTS raw CASCADE;

CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION analyticsuser;

-- create staging schema which is the staging (silver) layer for the data warehouse

CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION analyticsuser;

-- create dwh schema which is the dwh (gold) layer for the datawarehouse

CREATE SCHEMA IF NOT EXISTS dwh AUTHORIZATION analyticsuser;