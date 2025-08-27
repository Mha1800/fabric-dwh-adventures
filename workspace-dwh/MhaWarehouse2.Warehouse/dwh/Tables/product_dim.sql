CREATE TABLE [dwh].[product_dim] (

	[ProductSKU] varchar(8000) NULL, 
	[ProductName] varchar(8000) NULL, 
	[ModelName] varchar(8000) NULL, 
	[ProductDescription] varchar(8000) NULL, 
	[ProductColor] varchar(8000) NULL, 
	[ProductSize] varchar(8000) NULL, 
	[ProductStyle] varchar(8000) NULL, 
	[ProductId] int NULL, 
	[ProductPrice] decimal(18,0) NULL, 
	[ProductCost] decimal(18,0) NULL, 
	[SubcategoryName] varchar(1) NULL, 
	[CategoryName] varchar(1) NULL, 
	[valid_from_dttm] datetime2(6) NULL, 
	[valid_to_dttm] datetime2(6) NULL, 
	[processed_dttm] datetime2(6) NULL, 
	[product_rk] int NULL
);