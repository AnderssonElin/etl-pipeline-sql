/*
* File: 2_create_bulk_procedures.sql
* Description: Creates stored procedures for bulk loading data into staging tables
* Author: ETL Pipeline Project
* Date: 2023-03-04
*/

USE BI23_Stage
GO

-- Procedure to bulk insert Product data
CREATE PROCEDURE BULK_Production_Product
AS
BEGIN
    -- Bulk insert Product data from CSV file
    BULK INSERT sit.Production_Product 
    FROM 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\DIM_Adventure_Production_Product.csv'
    WITH(
        Fieldterminator = ',',
        Rowterminator = '\n',
        Firstrow = 2,
        FORMAT= 'csv',
        Formatfile = 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\Format_Product.txt'
    )
END
GO

-- Procedure to bulk insert Product Subcategory data
CREATE PROCEDURE BULK_Production_ProductSubcategory
AS
BEGIN
    -- Bulk insert Product Subcategory data from CSV file
    BULK INSERT sit.Production_ProductSubcategory 
    FROM 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\DIM_Adventure_Production_ProductSubcategory.csv'
    WITH(
        Fieldterminator = ',',
        Rowterminator = '\n',
        Firstrow = 2,
        FORMAT= 'csv',
        Formatfile = 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\Format_ProductSubcategory.txt'
    )
END
GO

-- Procedure to bulk insert Product Category data
CREATE PROCEDURE BULK_Production_ProductCategory
AS
BEGIN
    -- Bulk insert Product Category data from CSV file
    BULK INSERT sit.Production_ProductCategory 
    FROM 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\DIM_Adventure_Production_ProductCategory.csv'
    WITH(
        Fieldterminator = ',',
        Rowterminator = '\n',
        Firstrow = 2,
        FORMAT= 'csv',
        Formatfile = 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\Format_ProductCategory.txt'
    )
END
GO

-- Procedure to bulk insert Address data
CREATE PROCEDURE BULK_Person_Address
AS
BEGIN
    -- Bulk insert Address data from CSV file
    BULK INSERT sit.Person_Address
    FROM 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\DIM_Adventure_Person_Address.csv'
    WITH(
        Fieldterminator = ',',
        Rowterminator = '\n',
        Firstrow = 2,
        FORMAT= 'csv',
        Formatfile = 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\Format_Person_Address.txt'
    )
END
GO

-- Procedure to bulk insert Business Entity Address data
CREATE PROCEDURE BULK_Person_BusinessEntityAddress
AS
BEGIN
    -- Bulk insert Business Entity Address data from CSV file
    BULK INSERT sit.Person_BusinessEntityAddress
    FROM 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\DIM_Adventure_Person_BusinessEntityAddress.csv'
    WITH(
        Fieldterminator = ',',
        Rowterminator = '\n',
        Firstrow = 2,
        FORMAT= 'csv',
        Formatfile = 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\Format_Person_BusinessEntityAddress.txt'
    )
END
GO

-- Procedure to bulk insert Address Type data
CREATE PROCEDURE BULK_Person_AddressType
AS
BEGIN
    -- Bulk insert Address Type data from CSV file
    BULK INSERT sit.Person_AddressType
    FROM 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\DIM_Adventure_Person_AddressType.csv'
    WITH(
        Fieldterminator = ',',
        Rowterminator = '\n',
        Firstrow = 2,
        FORMAT= 'csv',
        Formatfile = 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\Format_Person_AddressType.txt'
    )
END
GO

-- Procedure to bulk insert Sales Order Header data
CREATE PROCEDURE BULK_SalesOrderHeader
AS
BEGIN
    -- Bulk insert Sales Order Header data from CSV file
    BULK INSERT sit.SalesOrderHeader 
    FROM 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\FACT_Adventure_Sales_SalesOrderHeader.csv'
    WITH(
        Fieldterminator = ',',
        Rowterminator = '\n',
        Firstrow = 2,
        FORMAT= 'csv',
        Formatfile = 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\Format_SalesOrderHeader.txt'
    )
END
GO

-- Procedure to bulk insert Sales Order Detail data
CREATE PROCEDURE BULK_SalesOrderDetail
AS
BEGIN
    -- Bulk insert Sales Order Detail data from CSV file
    BULK INSERT sit.SalesOrderDetail	 
    FROM 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\FACT_Adventure_Sales_SalesOrderDetail.csv'
    WITH(
        Fieldterminator = ',',
        Rowterminator = '\n',
        Firstrow = 2,
        FORMAT= 'csv',
        Formatfile = 'C:\Users\Thinkpad X13\OneDrive - Nackademin AB\Kurser\SQL\BI23DatafileSQL\Format_SalesOrderDetail.txt'
    )
END
GO 