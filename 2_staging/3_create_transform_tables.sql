/*
* File: 3_create_transform_tables.sql
* Description: Creates the transformation tables (STT schema) for transformed staging data
* Author: ETL Pipeline Project
* Date: 2023-03-04
*/

USE BI23_Stage
GO

-- Create transformation table for Product data
-- This table combines product, subcategory, and category information
CREATE TABLE stt.DimProduct (
    Product_skey INT NOT NULL IDENTITY(1,1),
    ProductID INT,
    ProductName NVARCHAR(255),
    ProductNumber NVARCHAR(255),
    Color NVARCHAR(255),
    StandardCost DECIMAL(18,2),
    ListPrice DECIMAL(18,2),
    Size NVARCHAR(255),
    SizeUnitMeasureCode NVARCHAR(255),
    WeightUnitMeasureCode NVARCHAR(255),
    [Weight] DECIMAL(18,2),
    ProductSubcategoryID INT,
    SellStartDate DATETIME,
    SellEndDate DATETIME,
    ModifiedDate DATETIME,
    Product_ts DATETIME,
    SubcategoryName NVARCHAR(255),
    ProductCategoryID INT,
    CategoryName NVARCHAR(255),
    ProductCat_ts DATETIME
);
GO

-- Create transformation table for Address data
-- This table combines address and address type information
CREATE TABLE stt.DimAddress (
    Address_skey INT NOT NULL IDENTITY(1,1),
    AddressID INT NOT NULL,
    AddressLine NVARCHAR(255),
    City NVARCHAR(255),
    PostalCode NVARCHAR(255),
    ModifiedDate DATETIME,
    Address_ts DATETIME,
    AddressTypeName NVARCHAR(255),
    AddressTypeModifiedDate DATETIME,
    AddressType_ts DATETIME
);
GO

-- Create transformation table for Fact Order data
-- This table combines order header and detail information with dimension keys
CREATE TABLE stt.FactOrder (
    Product_skey INT,
    Address_skey INT,
    SalesOrderID INT,
    OrderDate INT,
    DueDate INT,
    ShipDate INT,
    SalesOrderNumber NVARCHAR(255),
    ShipToAddressID INT,
    SubTotal DECIMAL(18,2),
    TaxAmt DECIMAL(18,2),
    Freight DECIMAL(18,2),
    TotalDue DECIMAL(18,2),
    OrderModifiedDate DATETIME,
    OrderHeader_ts DATETIME,
    SalesOrderDetailID INT,
    OrderQty SMALLINT,
    ProductID INT,
    SpecialOfferID INT,
    UnitPrice DECIMAL(18,2),
    UnitPriceDiscount DECIMAL(18,2),
    LineTotal DECIMAL(18,2),
    DetailModifiedDate DATETIME,
    OrderDetail_ts DATETIME
);
GO 