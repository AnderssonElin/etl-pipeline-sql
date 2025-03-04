/*
* File: execute_pipeline.sql
* Description: Script to execute the entire ETL pipeline
* Author: ETL Pipeline Project
* Date: 2023-03-04
*/

-- Step 1: Create databases and schemas
-- This step should be run only once when setting up the environment
PRINT 'Step 1: Creating databases and schemas (if not exists)';
:r ..\1_database_setup\1_create_databases.sql
:r ..\1_database_setup\2_create_schemas.sql
GO

-- Step 2: Create staging tables and bulk procedures
-- This step should be run only once when setting up the environment
PRINT 'Step 2: Creating staging tables and bulk procedures (if not exists)';
:r ..\2_staging\1_create_staging_tables.sql
:r ..\2_staging\2_create_bulk_procedures.sql
:r ..\2_staging\3_create_transform_tables.sql
GO

-- Step 3: Create data warehouse tables and views
-- This step should be run only once when setting up the environment
PRINT 'Step 3: Creating data warehouse tables and views (if not exists)';
:r ..\3_warehouse\1_create_dimension_tables.sql
:r ..\3_warehouse\2_create_fact_tables.sql
:r ..\3_warehouse\3_create_views.sql
GO

-- Step 4: Create ETL procedures
-- This step should be run only once when setting up the environment
PRINT 'Step 4: Creating ETL procedures (if not exists)';
:r ..\4_etl_procedures\1_merge_product.sql
:r ..\4_etl_procedures\2_merge_address.sql
:r ..\4_etl_procedures\3_fact_dim_join.sql
:r ..\4_etl_procedures\4_import_procedures.sql
GO

-- Step 5: Execute the ETL pipeline
-- This step can be run repeatedly to refresh the data warehouse
PRINT 'Step 5: Executing the ETL pipeline';

-- Step 5.1: Extract data from source to staging
PRINT '  Step 5.1: Extracting data from source to staging';
USE BI23_Stage;
GO

-- Execute bulk insert procedures to load data into staging tables
EXEC BULK_Production_Product;
EXEC BULK_Production_ProductSubcategory;
EXEC BULK_Production_ProductCategory;
EXEC BULK_Person_Address;
EXEC BULK_Person_BusinessEntityAddress;
EXEC BULK_Person_AddressType;
EXEC BULK_SalesOrderHeader;
EXEC BULK_SalesOrderDetail;
GO

-- Step 5.2: Transform data in staging
PRINT '  Step 5.2: Transforming data in staging';

-- Clear existing data in transformation tables
TRUNCATE TABLE stt.DimProduct;
TRUNCATE TABLE stt.DimAddress;
TRUNCATE TABLE stt.FactOrder;
GO

-- Execute transformation procedures
EXEC MergeProductTable;
EXEC MergeAddressTable;
EXEC FactAndDimJoin;
GO

-- Step 5.3: Load data to data warehouse
PRINT '  Step 5.3: Loading data to data warehouse';

-- Execute import procedures
EXEC ImportSttToDmtDimProduct;
EXEC ImportSttToDmtDimAddress;
EXEC ImportSttToDmtFactOrder;
GO

PRINT 'ETL pipeline execution completed successfully!';
GO 