CREATE TABLE [dwh].[product_category_dim] (

	[CategoryName] varchar(8000) NULL, 
	[ProductCategoryId] int NULL, 
	[valid_from_dttm] datetime2(6) NULL, 
	[valid_to_dttm] datetime2(6) NULL, 
	[processed_dttm] datetime2(6) NULL, 
	[product_category_rk] int NULL
);