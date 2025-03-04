/*
* File: 1_create_staging_tables.sql
* Description: Creates the staging tables (SIT schema) for initial data loading
* Author: ETL Pipeline Project
* Date: 2023-03-04
*/

USE BI23_Stage
GO

-- Create staging tables for Product data
CREATE TABLE sit.Production_Product  (				
	 [ProductID]				nvarchar(255)
	,[Name]						nvarchar(255)
	,[ProductNumber] 			nvarchar(255)
	,[MakeFlag]					nvarchar(255)
	,[FinishedGoodsFlag]		nvarchar(255)
	,[Color]					nvarchar(255)
	,[SafetyStockLevel]			nvarchar(255)
	,[ReorderPoint]				nvarchar(255)
	,[StandardCost]				nvarchar(255)
	,[ListPrice]				nvarchar(255)
	,[Size]						nvarchar(255)
	,[SizeUnitMeasureCode]		nvarchar(255)
	,[WeightUnitMeasureCode]	nvarchar(255)
	,[Weight]					nvarchar(255)
	,[DaysToManufacture]		nvarchar(255)
	,[ProductLine]				nvarchar(255)
	,[Class]					nvarchar(255)
	,[Style]					nvarchar(255)
	,[ProductSubcategoryID]		nvarchar(255)
	,[ProductModelID]			nvarchar(255)
	,[SellStartDate]			nvarchar(255)
	,[SellEndDate]				nvarchar(255)
	,[DiscontinuedDate]			nvarchar(255)
	,[rowguid]					nvarchar(255)
	,[ModifiedDate]				nvarchar(255)
	,[Product_ts]				datetime default getdate()
)

-- Create staging table for Product Subcategory data
CREATE TABLE sit.Production_ProductSubcategory (
	 [ProductSubcategoryID]	nvarchar(255)
	,[ProductCategoryID]	nvarchar(255)
	,[Name]					nvarchar(255)
	,[rowguid]				nvarchar(255)
	,[ModifiedDate]			nvarchar(255)
	,[ProductSubcat_ts]		datetime default getdate()
)

-- Create staging table for Product Category data
CREATE TABLE sit.Production_ProductCategory (
	 [ProductCategoryID]	nvarchar(255)
	,[Name]					nvarchar(255)
	,[rowguid]				nvarchar(255)
	,[ModifiedDate]			nvarchar(255)
	,[ProductCat_ts]		datetime default getdate()
)

-- Create staging table for Address data
CREATE TABLE sit.Person_Address (
	 [AddressID]			nvarchar(255)
	,[AddressLine1]			nvarchar(255)
	,[AddressLine2]			nvarchar(255)
	,[City]					nvarchar(255)
	,[StateProvinceID]		nvarchar(255)
	,[PostalCode]			nvarchar(255)
	,[SpatialLocation]		nvarchar(255)
	,[rowguid]				nvarchar(255)
	,[ModifiedDate]			nvarchar(255)
	,[Address_ts]			datetime default getdate()
)

-- Create staging table for Business Entity Address data
CREATE TABLE sit.Person_BusinessEntityAddress (
	[BusinessEntityID]			nvarchar(255)
	, [AddressID]				nvarchar(255)
	, [AddressTypeID]			nvarchar(255)
	, [rowguid]					nvarchar(255)
	, [ModifiedDate]			nvarchar(255)
	, [BussinessAddress_ts]		datetime default getdate()
	)

-- Create staging table for Address Type data
CREATE TABLE sit.Person_AddressType (
	 [AddressTypeID]			nvarchar(255)
	,[Name]						nvarchar(255)
	,[rowguid]					nvarchar(255)
	,[ModifiedDate]				nvarchar(255)
	,[AddressType_ts]			datetime default getdate()
	)

-- Create staging table for Sales Order Header data
CREATE TABLE sit.SalesOrderHeader (
	  [SalesOrderID]			nvarchar(255)
	, [RevisionNumber]			nvarchar(255)
	, [OrderDate]				nvarchar(255)
	, [DueDate]					nvarchar(255)
	, [ShipDate]				nvarchar(255)
	, [Status]					nvarchar(255)
	, [OnlineOrderFlag]			nvarchar(255)
	, [SalesOrderNumber]		nvarchar(255)
	, [PurchaseOrderNumber]		nvarchar(255)
	, [AccountNumber]			nvarchar(255)
	, [CustomerID]				nvarchar(255)
	, [SalesPersonID]			nvarchar(255)
	, [TerritoryID]				nvarchar(255)
	, [BillToAddressID]			nvarchar(255)
	, [ShipToAddressID]			nvarchar(255)
	, [ShipMethodID]			nvarchar(255)
	, [CreditCardID]			nvarchar(255)
	, [CreditCardApprovalCode]	nvarchar(255)
	, [CurrencyRateID]			nvarchar(255)
	, [SubTotal]				nvarchar(255)
	, [TaxAmt]					nvarchar(255)
	, [Freight]					nvarchar(255)
	, [TotalDue]				nvarchar(255)
	, [Comment]					nvarchar(255)
	, [rowguid]					nvarchar(255)
	, [ModifiedDate]			nvarchar(255)
	, [OrderHeader_ts]			datetime default getdate()
	)

-- Create staging table for Sales Order Detail data
CREATE TABLE sit.SalesOrderDetail (
	   [SalesOrderID]				nvarchar(255)
	  ,[SalesOrderDetailID]			nvarchar(255)
      ,[CarrierTrackingNumber]		nvarchar(255)
      ,[OrderQty]					nvarchar(255)
      ,[ProductID]					nvarchar(255)
      ,[SpecialOfferID]				nvarchar(255)
      ,[UnitPrice]					nvarchar(255)
      ,[UnitPriceDiscount]			nvarchar(255)
      ,[LineTotal]					nvarchar(255)
      ,[rowguid]					nvarchar(255)
      ,[ModifiedDate]				nvarchar(255)
	  ,[OrderDetail_ts]				datetime default getdate()
	)
GO 