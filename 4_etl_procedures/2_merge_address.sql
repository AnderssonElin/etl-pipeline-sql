/*
* File: 2_merge_address.sql
* Description: Procedure to merge address data from staging to transformation tables
* Author: ETL Pipeline Project
* Date: 2023-03-04
*/

USE BI23_Stage
GO

-- Create procedure to merge address data
-- This procedure combines address and address type data
-- It handles unknown values and performs data type conversions
CREATE PROCEDURE MergeAddressTable
AS
BEGIN
    -- Create a temporary table with transformed address data
    -- Join address with business entity address and address type tables
    SELECT 
        TRY_CAST(pa.AddressID AS INT) AS AddressID,
        pa.AddressLine1 AS AddressLine,
        pa.City AS City,
        pa.PostalCode AS PostalCode,
        TRY_CAST(pa.ModifiedDate AS DATETIME) AS ModifiedDate,
        TRY_CAST(pa.Address_ts AS DATETIME) AS Address_ts,
        pat.Name AS AddressTypeName,
        TRY_CAST(pat.ModifiedDate AS DATETIME) AS AddressTypeModifiedDate,
        TRY_CAST(pat.AddressType_ts AS DATETIME) AS AddressType_ts
    INTO #TempAddressWithType 
    FROM 
        sit.Person_Address pa
    LEFT JOIN 
        sit.Person_BusinessEntityAddress pbea ON pa.AddressID = pbea.AddressID
    LEFT JOIN 
        sit.Person_AddressType pat ON pbea.AddressTypeID = pat.AddressTypeID;

    -- Turn off identity insert to allow manual entry of primary key values
    SET IDENTITY_INSERT stt.DimAddress ON;

    -- Insert unknown row for handling missing references
    INSERT INTO stt.DimAddress (
        Address_skey,
        AddressID,
        AddressLine,
        City,
        PostalCode,
        ModifiedDate,
        Address_ts,
        AddressTypeName,
        AddressTypeModifiedDate,
        AddressType_ts
    )
    VALUES (
        -1,         -- Unknown Address_skey
        -1,         -- Unknown AddressID
        'Unknown',  -- Unknown AddressLine1
        'Unknown',  -- Unknown City
        'Unknown',  -- Unknown PostalCode
        NULL,       -- Unknown AddressModifiedDate
        NULL,       -- Unknown Address_ts
        'Unknown',  -- Unknown AddressTypeName
        GETDATE(),  -- Unknown AddressTypeModifiedDate
        GETDATE()   -- Unknown AddressType_ts
    );

    -- Reactivate identity insert
    SET IDENTITY_INSERT stt.DimAddress OFF;

    -- Merge data from temporary table to transformation table
    -- Handle both new records and updates to existing records
    MERGE INTO stt.DimAddress AS target
    USING #TempAddressWithType AS source
    ON target.AddressID = source.AddressID 

    -- Handle new records
    WHEN NOT MATCHED BY TARGET
    THEN INSERT (
        AddressID,
        AddressLine,
        City,
        PostalCode,
        ModifiedDate,
        Address_ts,
        AddressTypeName,
        AddressTypeModifiedDate,
        AddressType_ts
    )
    VALUES (
        source.AddressID,
        source.AddressLine,
        source.City,
        source.PostalCode,
        source.ModifiedDate,
        source.Address_ts,
        source.AddressTypeName,
        source.AddressTypeModifiedDate,
        source.AddressType_ts
    )

    -- Handle updates to existing records
    WHEN MATCHED AND (
        source.AddressID <> target.AddressID				
        OR source.AddressLine <> target.AddressLine			
        OR source.City <> target.City					
        OR source.PostalCode <> target.PostalCode				
        OR source.ModifiedDate <> target.ModifiedDate			
        OR source.Address_ts <> target.Address_ts				
        OR source.AddressTypeName <> target.AddressTypeName		
        OR source.AddressTypeModifiedDate <> target.AddressTypeModifiedDate
        OR source.AddressType_ts <> target.AddressType_ts
    )
    THEN UPDATE SET
        target.AddressID = source.AddressID,
        target.AddressLine = source.AddressLine,
        target.City = source.City,
        target.PostalCode = source.PostalCode,
        target.ModifiedDate = source.ModifiedDate,
        target.Address_ts = source.Address_ts,
        target.AddressTypeName = source.AddressTypeName,
        target.AddressTypeModifiedDate = source.AddressTypeModifiedDate,
        target.AddressType_ts = source.AddressType_ts;

    -- Clean up temporary table
    DROP TABLE #TempAddressWithType;
END
GO 