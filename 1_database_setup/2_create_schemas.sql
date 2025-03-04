/*
* File: 2_create_schemas.sql
* Description: Creates the necessary schemas in both databases
* Author: ETL Pipeline Project
* Date: 2023-03-04
*/

-- Use the Data Warehouse database
USE BI23_DW
GO

-- Create schemas in the Data Warehouse
-- DMT: Data Mart Tables (physical tables)
CREATE SCHEMA dmt
GO

-- DMV: Data Mart Views (logical views)
CREATE SCHEMA dmv
GO

-- Use the Staging database
USE BI23_Stage
GO

-- Create schemas in the Staging database
-- SIT: Source Integration Tables (initial staging)
CREATE SCHEMA sit
GO

-- STT: Staging Transformation Tables (transformed staging)
CREATE SCHEMA stt
GO 