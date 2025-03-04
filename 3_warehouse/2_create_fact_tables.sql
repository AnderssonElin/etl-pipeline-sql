/*
* File: 2_create_fact_tables.sql
* Description: Creates fact tables in the data warehouse
* Author: ETL Pipeline Project
* Date: 2023-03-04
*/

USE BI23_DW
GO

-- Create Fact Order table in the data warehouse
-- This table stores sales order facts with references to dimension tables
CREATE TABLE dmt.FactOrder (
    -- Dimension keys
    Product_skey INT,
    Address_skey INT,
    
    -- Order header attributes
    SalesOrderID INT,
    OrderDate INT,  -- DateKey format (YYYYMMDD)
    DueDate INT,    -- DateKey format (YYYYMMDD)
    ShipDate INT,   -- DateKey format (YYYYMMDD)
    SalesOrderNumber NVARCHAR(255),
    ShipToAddressID INT,
    
    -- Financial measures
    SubTotal DECIMAL(18,2),
    TaxAmt DECIMAL(18,2),
    Freight DECIMAL(18,2),
    TotalDue DECIMAL(18,2),
    
    -- Order detail attributes
    SalesOrderDetailID INT,
    OrderQty SMALLINT,
    ProductID INT,
    SpecialOfferID INT,
    UnitPrice DECIMAL(18,2),
    UnitPriceDiscount DECIMAL(18,2),
    LineTotal DECIMAL(18,2),
    
    -- Audit timestamps
    OrderHeader_ts DATETIME,
    OrderDetail_ts DATETIME,
    dmt_FactSales_ts DATETIME
);
GO 