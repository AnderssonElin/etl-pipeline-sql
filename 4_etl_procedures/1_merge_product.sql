/*
* File: 1_merge_product.sql
* Description: Procedure to merge product data from staging to transformation tables
* Author: ETL Pipeline Project
* Date: 2023-03-04
*/

USE BI23_Stage
GO

-- Create procedure to merge product data
-- This procedure combines product, subcategory, and category data
-- It handles unknown values and performs data type conversions
CREATE PROCEDURE MergeProductTable
AS
BEGIN
    -- Create a temporary table with transformed product data
    -- Join product with subcategory and category tables
    SELECT 
        TRY_CAST(p.ProductID AS INT) AS ProductID,
        p.Name AS ProductName,
        p.ProductNumber AS ProductNumber,
        p.Color AS Color,
        TRY_CAST(p.StandardCost AS DECIMAL(18,2)) AS StandardCost,
        TRY_CAST(p.ListPrice AS DECIMAL(18,2)) AS ListPrice,
        p.Size AS Size,
        p.SizeUnitMeasureCode AS SizeUnitMeasureCode,
        p.WeightUnitMeasureCode AS WeightUnitMeasureCode,
        TRY_CAST(p.[Weight] AS DECIMAL(18,2)) AS [Weight],
        TRY_CAST(p.ProductSubcategoryID AS INT) AS ProductSubcategoryID,
        TRY_CAST(p.SellStartDate AS DATETIME) AS SellStartDate,
        COALESCE(TRY_CAST(p.SellEndDate AS DATETIME), '20990101') AS SellEndDate,
        TRY_CAST(p.ModifiedDate AS DATETIME) AS ModifiedDate,
        TRY_CAST(p.Product_ts AS DATETIME) AS Product_ts,
        ps.Name AS SubcategoryName,
        TRY_CAST(pc.ProductCategoryID AS INT) AS ProductCategoryID,
        pc.Name AS CategoryName,
        TRY_CAST(pc.ProductCat_ts AS DATETIME) AS ProductCat_ts
    INTO #TempProduct
    FROM 
        sit.Production_Product p
    LEFT JOIN 
        sit.Production_ProductSubcategory ps ON ps.ProductSubcategoryID = p.ProductSubcategoryID
    LEFT JOIN
        sit.Production_ProductCategory pc ON pc.ProductCategoryID = ps.ProductCategoryID;

    -- Turn off identity insert to allow manual entry of primary key values
    SET IDENTITY_INSERT stt.DimProduct ON;

    -- Insert unknown row for handling missing references
    INSERT INTO stt.DimProduct (
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
        [Weight],
        ProductSubcategoryID,
        SellStartDate,
        SellEndDate,
        ModifiedDate,
        Product_ts,
        SubcategoryName,
        ProductCategoryID,
        CategoryName,
        ProductCat_ts
    )
    VALUES (
        -1,             -- Unknown Product_skey		
        -1,             -- Unknown ProductID
        'Unknown',      -- Unknown ProductName
        'Unknown',      -- Unknown ProductNumber	
        'Unknown',      -- Unknown Color
        NULL,           -- Unknown StandardCost
        NULL,           -- Unknown ListPrice
        'Unknown',      -- Unknown Size
        'Unknown',      -- Unknown SizeUnitMeasureCode	
        'Unknown',      -- Unknown WeightUnitMeasureCode	
        NULL,           -- Unknown Weight				
        NULL,           -- Unknown ProductSubcategoryID	
        '1900-01-01',   -- Unknown SellStartDate			
        '2099-01-01',   -- Unknown SellEndDate			
        GETDATE(),      -- Unknown ModifiedDate			
        GETDATE(),      -- Unknown Product_ts		
        'Unknown',      -- Unknown SubcategoryName		
        NULL,           -- Unknown ProductCategoryID		
        'Unknown',      -- Unknown CategoryName			
        GETDATE()       -- Unknown ProductCat_ts
    );

    -- Reactivate identity insert
    SET IDENTITY_INSERT stt.DimProduct OFF;

    -- Merge data from temporary table to transformation table
    -- Handle both new records and updates to existing records
    MERGE INTO stt.DimProduct AS target
    USING #TempProduct AS source
    ON target.ProductID = source.ProductID 

    -- Handle new records
    WHEN NOT MATCHED BY TARGET 
    THEN INSERT ( 
        ProductID,
        ProductName,
        ProductNumber,
        Color, 
        StandardCost,
        ListPrice,
        Size,
        SizeUnitMeasureCode,
        WeightUnitMeasureCode,
        [Weight],
        ProductSubcategoryID,
        SellStartDate,
        SellEndDate,
        ModifiedDate,
        Product_ts,
        SubcategoryName,
        ProductCategoryID,
        CategoryName,
        ProductCat_ts
    )
    VALUES ( 
        source.ProductID,
        source.ProductName,
        source.ProductNumber,
        source.Color,
        source.StandardCost,
        source.ListPrice,
        source.Size,
        source.SizeUnitMeasureCode,
        source.WeightUnitMeasureCode,
        source.[Weight],
        source.ProductSubcategoryID,
        source.SellStartDate,
        source.SellEndDate,
        source.ModifiedDate,
        source.Product_ts,
        source.SubcategoryName,
        source.ProductCategoryID,
        source.CategoryName,
        source.ProductCat_ts
    )

    -- Handle updates to existing records
    WHEN MATCHED AND (
        source.ProductID <> target.ProductID			
        OR source.ProductName <> target.ProductName			
        OR source.ProductNumber <> target.ProductNumber		
        OR source.Color <> target.Color				
        OR source.StandardCost <> target.StandardCost			
        OR source.ListPrice <> target.ListPrice			
        OR source.Size <> target.Size					
        OR source.SizeUnitMeasureCode <> target.SizeUnitMeasureCode	
        OR source.WeightUnitMeasureCode <> target.WeightUnitMeasureCode
        OR source.[Weight] <> target.[Weight]				
        OR source.ProductSubcategoryID <> target.ProductSubcategoryID	
        OR source.SellStartDate <> target.SellStartDate		
        OR source.SellEndDate <> target.SellEndDate			
        OR source.ModifiedDate <> target.ModifiedDate			
        OR source.Product_ts <> target.Product_ts		
        OR source.SubcategoryName <> target.SubcategoryName		
        OR source.ProductCategoryID <> target.ProductCategoryID	
        OR source.CategoryName <> target.CategoryName			
        OR source.ProductCat_ts <> target.ProductCat_ts
    )
    THEN UPDATE SET
        target.ProductID = source.ProductID,
        target.ProductName = source.ProductName,
        target.ProductNumber = source.ProductNumber,
        target.Color = source.Color,
        target.StandardCost = source.StandardCost,
        target.ListPrice = source.ListPrice,
        target.Size = source.Size,
        target.SizeUnitMeasureCode = source.SizeUnitMeasureCode,
        target.WeightUnitMeasureCode = source.WeightUnitMeasureCode,
        target.[Weight] = source.[Weight],
        target.ProductSubcategoryID = source.ProductSubcategoryID,
        target.SellStartDate = source.SellStartDate,
        target.SellEndDate = source.SellEndDate,
        target.ModifiedDate = source.ModifiedDate,
        target.Product_ts = source.Product_ts,
        target.SubcategoryName = source.SubcategoryName,
        target.ProductCategoryID = source.ProductCategoryID,
        target.CategoryName = source.CategoryName,
        target.ProductCat_ts = source.ProductCat_ts;

    -- Clean up temporary table
    DROP TABLE #TempProduct;
END
GO 