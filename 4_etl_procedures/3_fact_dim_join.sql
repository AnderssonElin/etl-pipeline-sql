/*
* File: 3_fact_dim_join.sql
* Description: Procedure to join fact and dimension data for the fact table
* Author: ETL Pipeline Project
* Date: 2023-03-04
*/

USE BI23_Stage
GO

-- Create procedure to join fact and dimension data
-- This procedure combines sales order header and detail data
-- and links it to product and address dimensions
CREATE PROCEDURE FactAndDimJoin
AS
BEGIN
    -- Create a temporary table with transformed fact data
    -- Join sales order header and detail tables
    -- Link to product and address dimensions
    -- Convert date fields to DateKey format
    SELECT 
        dp.Product_skey AS Product_skey,
        da.Address_skey AS Address_skey,
        TRY_CAST(soh.SalesOrderID AS INT) AS SalesOrderID, 
        TRY_CAST(CONVERT(VARCHAR(8), CAST(soh.OrderDate AS DATETIME), 112) AS INT) AS OrderDate,
        TRY_CAST(CONVERT(VARCHAR(8), CAST(soh.DueDate AS DATETIME), 112) AS INT) AS DueDate,
        TRY_CAST(CONVERT(VARCHAR(8), CAST(soh.ShipDate AS DATETIME), 112) AS INT) AS ShipDate,
        soh.SalesOrderNumber AS SalesOrderNumber,
        TRY_CAST(soh.ShipToAddressID AS INT) AS ShipToAddressID,
        TRY_CAST(soh.SubTotal AS DECIMAL(18,2)) AS SubTotal,
        TRY_CAST(soh.TaxAmt AS DECIMAL(18,2)) AS TaxAmt,
        TRY_CAST(soh.Freight AS DECIMAL(18,2)) AS Freight,
        TRY_CAST(soh.TotalDue AS DECIMAL(18,2)) AS TotalDue,
        TRY_CAST(soh.ModifiedDate AS DATETIME) AS OrderModifiedDate,
        TRY_CAST(soh.OrderHeader_ts AS DATETIME) AS OrderHeader_ts,
        TRY_CAST(sod.SalesOrderID AS INT) AS SalesOrderDetailID,
        TRY_CAST(sod.OrderQty AS SMALLINT) AS OrderQty,
        TRY_CAST(sod.ProductID AS INT) AS ProductID,
        TRY_CAST(sod.SpecialOfferID AS INT) AS SpecialOfferID,
        TRY_CAST(sod.UnitPrice AS DECIMAL(18,2)) AS UnitPrice,
        TRY_CAST(sod.UnitPriceDiscount AS DECIMAL(18,2)) AS UnitPriceDiscount,
        TRY_CAST(sod.LineTotal AS DECIMAL(18,2)) AS LineTotal,
        TRY_CAST(sod.ModifiedDate AS DATETIME) AS DetailModifiedDate,
        TRY_CAST(sod.OrderDetail_ts AS DATETIME) AS OrderDetail_ts
    INTO 
        #TempFactOrder
    FROM 
        sit.SalesOrderHeader soh
    LEFT JOIN 
        sit.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
    LEFT JOIN 
        stt.DimProduct dp ON dp.ProductID = sod.ProductID
    LEFT JOIN
        stt.DimAddress da ON da.AddressID = soh.ShipToAddressID
    LEFT JOIN
        BI23_DW.dmt.DimDate ddOrder ON ddOrder.DateKey = soh.OrderDate
    LEFT JOIN 
        BI23_DW.dmt.DimDate ddDue ON ddDue.DateKey = soh.DueDate
    LEFT JOIN 
        BI23_DW.dmt.DimDate ddShip ON ddShip.DateKey = soh.ShipDate;

    -- Insert data from temporary table to fact table
    INSERT INTO stt.FactOrder (	
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
        OrderModifiedDate,
        OrderHeader_ts,
        SalesOrderDetailID,
        OrderQty,
        ProductID,
        SpecialOfferID,
        UnitPrice,
        UnitPriceDiscount,
        LineTotal,
        DetailModifiedDate,
        OrderDetail_ts
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
        OrderModifiedDate,
        OrderHeader_ts,
        SalesOrderDetailID,
        OrderQty,
        ProductID,
        SpecialOfferID,
        UnitPrice,
        UnitPriceDiscount,
        LineTotal,
        DetailModifiedDate,
        OrderDetail_ts
    FROM 
        #TempFactOrder;

    -- Clean up temporary table
    DROP TABLE #TempFactOrder;
END
GO 