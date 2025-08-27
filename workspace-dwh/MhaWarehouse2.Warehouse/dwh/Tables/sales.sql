CREATE TABLE [dwh].[sales] (

	[OrderDate] varchar(8000) NULL, 
	[StockDate] varchar(8000) NULL, 
	[OrderNumber] varchar(8000) NULL, 
	[OrderLineItem] varchar(8000) NULL, 
	[OrderQuantity] varchar(8000) NULL, 
	[ProductId] int NULL, 
	[CustomerId] int NULL, 
	[TerritoryId] int NULL, 
	[valid_from_dttm] datetime2(6) NULL, 
	[valid_to_dttm] datetime2(6) NULL, 
	[processed_dttm] datetime2(6) NULL, 
	[product_rk] int NULL, 
	[customer_rk] int NULL, 
	[territory_rk] int NULL
);