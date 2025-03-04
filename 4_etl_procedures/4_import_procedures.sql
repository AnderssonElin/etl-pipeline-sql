/*
* File: 4_import_procedures.sql
* Description: Procedures to import data from staging to the data warehouse
* Author: ETL Pipeline Project
* Date: 2023-03-04
*/

USE BI23_Stage
GO

-- Create procedure to import Product dimension data to the data warehouse
CREATE PROCEDURE ImportSttToDmtDimProduct
AS
BEGIN
    -- Check if the target table exists, create it if it doesn't
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                  WHERE TABLE_SCHEMA = 'dmt' AND TABLE_NAME = 'DimProduct'
                  AND TABLE_CATALOG = 'BI23_DW')
    BEGIN
        -- Create the table in the data warehouse
        EXECUTE('
            USE BI23_DW;
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
        ');
    END
    ELSE
    BEGIN
        -- Truncate the table if it exists
        EXECUTE('
            USE BI23_DW;
            TRUNCATE TABLE dmt.DimProduct;
        ');
    END;

    -- Insert data from staging to data warehouse
    INSERT INTO BI23_DW.dmt.DimProduct (
        Product_skey,
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
        CategoryName,
        Product_ts,
        ProductCat_ts,
        dmt_DimProduct_ts 
    )
    SELECT
        Product_skey,
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
        CategoryName,
        Product_ts,
        ProductCat_ts,
        GETDATE() -- Current timestamp for data warehouse load
    FROM
        stt.DimProduct;
END
GO

-- Create procedure to import Address dimension data to the data warehouse
CREATE PROCEDURE ImportSttToDmtDimAddress
AS
BEGIN
    -- Check if the target table exists, create it if it doesn't
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                  WHERE TABLE_SCHEMA = 'dmt' AND TABLE_NAME = 'DimAddress'
                  AND TABLE_CATALOG = 'BI23_DW')
    BEGIN
        -- Create the table in the data warehouse
        EXECUTE('
            USE BI23_DW;
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
        ');
    END
    ELSE
    BEGIN
        -- Truncate the table if it exists
        EXECUTE('
            USE BI23_DW;
            TRUNCATE TABLE dmt.DimAddress;
        ');
    END;

    -- Insert data from staging to data warehouse
    INSERT INTO BI23_DW.dmt.DimAddress (
        Address_skey,
        AddressID,
        AddressLine,
        City,
        PostalCode,
        AddressTypeName,
        Address_ts,
        AddressType_ts,
        dmt_DimAddress_ts
    )
    SELECT
        Address_skey,
        AddressID,
        AddressLine,
        City,
        PostalCode,
        AddressTypeName,
        Address_ts,
        AddressType_ts,
        GETDATE() -- Current timestamp for data warehouse load
    FROM
        stt.DimAddress;
END
GO

-- Create procedure to import Fact Order data to the data warehouse
CREATE PROCEDURE ImportSttToDmtFactOrder
AS
BEGIN
    -- Check if the target table exists, create it if it doesn't
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                  WHERE TABLE_SCHEMA = 'dmt' AND TABLE_NAME = 'FactOrder'
                  AND TABLE_CATALOG = 'BI23_DW')
    BEGIN
        -- Create the table in the data warehouse
        EXECUTE('
            USE BI23_DW;
            CREATE TABLE dmt.FactOrder (
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
                SalesOrderDetailID INT,
                OrderQty SMALLINT,
                ProductID INT,
                SpecialOfferID INT,
                UnitPrice DECIMAL(18,2),
                UnitPriceDiscount DECIMAL(18,2),
                LineTotal DECIMAL(18,2),
                OrderHeader_ts DATETIME,
                OrderDetail_ts DATETIME,
                dmt_FactSales_ts DATETIME
            );
        ');
    END
    ELSE
    BEGIN
        -- Truncate the table if it exists
        EXECUTE('
            USE BI23_DW;
            TRUNCATE TABLE dmt.FactOrder;
        ');
    END;

    -- Insert data from staging to data warehouse
    INSERT INTO BI23_DW.dmt.FactOrder (
        Product_skey,
        Address_skey,
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
        LineTotal,
        OrderHeader_ts,
        OrderDetail_ts,
        dmt_FactSales_ts
    )
    SELECT
        Product_skey,
        Address_skey,
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
        LineTotal,
        OrderHeader_ts,
        OrderDetail_ts,
        GETDATE() -- Current timestamp for data warehouse load
    FROM
        stt.FactOrder;
END
GO 