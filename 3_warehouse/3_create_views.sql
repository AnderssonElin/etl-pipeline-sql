/*
* File: 3_create_views.sql
* Description: Creates views for simplified data access in the data warehouse
* Author: ETL Pipeline Project
* Date: 2023-03-04
*/

USE BI23_DW
GO

-- Create Product dimension view
-- This view provides a simplified interface to the Product dimension
CREATE VIEW dmv.DimProduct AS
SELECT 
    ProductID,
    ProductName,
    ProductNumber,
    Color,
    StandardCost,
    ListPrice,
    Size,
    SizeUnitMeasureCode,
    WeightUnitMeasureCode,
    Weight,
    ProductSubcategoryID,
    SellStartDate,
    SellEndDate,
    SubcategoryName,
    ProductCategoryID,
    CategoryName
FROM 
    dmt.DimProduct;
GO

-- Create Address dimension view
-- This view provides a simplified interface to the Address dimension
CREATE VIEW dmv.DimAddress AS
SELECT
    AddressID,
    AddressLine,
    City,
    PostalCode,
    AddressTypeName
FROM 
    dmt.DimAddress;
GO

-- Create Date dimension view
-- This view provides a simplified interface to the Date dimension
CREATE VIEW dmv.DimDate AS
SELECT
    [DateKey], 
    [Date], 
    [WeekDay], 
    [Week], 
    [Month], 
    [Year]
FROM 
    dmt.DimDate;
GO

-- Create Fact Order view
-- This view provides a simplified interface to the Fact Order table
CREATE VIEW dmv.FactOrder AS
SELECT
    SalesOrderID,
    OrderDate,
    DueDate,
    ShipDate,
    SalesOrderNumber,
    ShipToAddressID,
    SubTotal,
    TaxAmt,
    Freight,
    TotalDue,
    SalesOrderDetailID,
    OrderQty,
    ProductID,
    SpecialOfferID,
    UnitPrice,
    UnitPriceDiscount,
    LineTotal
FROM 
    dmt.FactOrder;
GO 