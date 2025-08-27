CREATE TABLE [dwh].[sales] (

	[OrderDate] date NULL, 
	[product_rk] int NULL, 
	[customer_rk] int NULL, 
	[territory_rk] int NULL, 
	[ProductId] int NULL, 
	[CustomerId] int NULL, 
	[TerritoryId] int NULL, 
	[StockDate] date NULL, 
	[OrderNumber] varchar(1) NULL, 
	[OrderLineItem] decimal(18,0) NULL, 
	[OrderQuantity] decimal(18,0) NULL, 
	[processed_dttm] datetime2(6) NULL
);