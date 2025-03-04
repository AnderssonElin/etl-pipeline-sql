# SQL ETL Pipeline

This project implements a complete ETL (Extract, Transform, Load) pipeline using SQL Server. The pipeline extracts data from the AdventureWorks database, transforms it through a staging area, and loads it into a data warehouse for business intelligence purposes.

## Project Structure

The project is organized into the following components:

```
├── 1_database_setup/
│   ├── 1_create_databases.sql       # Creates the data warehouse and staging databases
│   └── 2_create_schemas.sql         # Creates the necessary schemas in both databases
├── 2_staging/
│   ├── 1_create_staging_tables.sql  # Creates the staging tables (SIT schema)
│   ├── 2_create_bulk_procedures.sql # Creates procedures for bulk loading data
│   └── 3_create_transform_tables.sql # Creates the transformation tables (STT schema)
├── 3_warehouse/
│   ├── 1_create_dimension_tables.sql # Creates dimension tables in the data warehouse
│   ├── 2_create_fact_tables.sql      # Creates fact tables in the data warehouse
│   └── 3_create_views.sql            # Creates views for simplified data access
├── 4_etl_procedures/
│   ├── 1_merge_product.sql           # Procedure to merge product data
│   ├── 2_merge_address.sql           # Procedure to merge address data
│   ├── 3_fact_dim_join.sql           # Procedure to join fact and dimension data
│   └── 4_import_procedures.sql       # Procedures to import data to the warehouse
└── 5_execution/
    └── execute_pipeline.sql          # Script to execute the entire ETL pipeline
```

## ETL Process Flow

1. **Extract**: Data is extracted from the AdventureWorks database and loaded into staging tables (SIT schema) using bulk insert procedures.
2. **Transform**: Data is transformed and merged in the staging area, handling unknown values and data type conversions, and stored in transformation tables (STT schema).
3. **Load**: Transformed data is loaded into the data warehouse dimension and fact tables (DMT schema).
4. **Access**: Views are created in the DMV schema to provide simplified access to the data warehouse.

## Dimension Tables

- **DimDate**: Calendar dimension with date attributes
- **DimProduct**: Product dimension with hierarchical product categories
- **DimAddress**: Address dimension with address types

## Fact Tables

- **FactOrder**: Sales order facts with references to dimensions

## How to Use

1. Execute the scripts in the numbered order of folders and files
2. The execution script in the 5_execution folder can be used to run the entire pipeline
3. After execution, data can be accessed through the views in the DMV schema

## Data Flow Diagram

```
AdventureWorks DB → SIT Schema → STT Schema → DMT Schema → DMV Schema
    (Source)      (Staging)   (Transform)   (Warehouse)    (Views)
```

## Requirements

- SQL Server 2016 or later
- AdventureWorks sample database
- Appropriate file paths for bulk insert operations (modify as needed)

## Notes

- The pipeline includes error handling and unknown value management
- Incremental loading is supported through merge operations
- Time stamps are maintained throughout the pipeline for auditing purposes 