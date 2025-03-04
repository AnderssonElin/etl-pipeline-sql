/*
* File: 1_create_dimension_tables.sql
* Description: Creates dimension tables in the data warehouse
* Author: ETL Pipeline Project
* Date: 2023-03-04
*/

USE BI23_DW
GO

-- Create Date dimension table
-- This table stores calendar date attributes for time-based analysis
CREATE TABLE BI23_DW.dmt.DimDate (
    [DateKey] int NOT NULL PRIMARY KEY,
    [Date] date NOT NULL,
    [WeekDay] nvarchar(10),
    [Week] int,
    [Month] nvarchar(10),
    [Year] int 
);
GO

-- Populate the Date dimension with dates from 2010 to 2015
-- This covers the range of dates in the sales data
DECLARE @startDate date = '20100101';
DECLARE @endDate date = '20151231';
DECLARE @currentDate date = @startDate;
DECLARE @dateKey int = 0;

WHILE @currentDate <= @endDate
BEGIN
    SET @dateKey = CAST(REPLACE(CONVERT(varchar, @currentDate, 112), '-', '') AS int);

    INSERT INTO BI23_DW.dmt.DimDate (
        [DateKey], 
        [Date], 
        [WeekDay], 
        [Week], 
        [Month], 
        [Year]
    )
    VALUES (
        @dateKey,
        @currentDate,
        DATENAME(WEEKDAY, @currentDate),
        DATEPART(WEEK, @currentDate),
        DATENAME(MONTH, @currentDate),
        YEAR(@currentDate)
    );

    SET @currentDate = DATEADD(DAY, 1, @currentDate);
END;

-- Set the first day of the week to Monday for consistent week calculations
SET DATEFIRST 1;
GO

-- Create Product dimension table in the data warehouse
-- This table will be populated from the staging transformation table
CREATE TABLE dmt.DimProduct (
    Product_skey INT NOT NULL,
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
    SubcategoryName NVARCHAR(255),
    ProductCategoryID INT,
    CategoryName NVARCHAR(255),
    Product_ts DATETIME,
    ProductCat_ts DATETIME,
    dmt_DimProduct_ts DATETIME
);
GO

-- Create Address dimension table in the data warehouse
-- This table will be populated from the staging transformation table
CREATE TABLE dmt.DimAddress (
    Address_skey INT NOT NULL,
    AddressID INT NOT NULL,
    AddressLine NVARCHAR(255),
    City NVARCHAR(255),
    PostalCode NVARCHAR(255),
    ModifiedDate DATETIME,
    AddressTypeName NVARCHAR(255),
    Address_ts DATETIME,
    AddressType_ts DATETIME,
    dmt_DimAddress_ts DATETIME
);
GO 