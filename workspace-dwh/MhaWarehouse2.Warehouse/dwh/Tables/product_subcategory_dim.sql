CREATE TABLE [dwh].[product_subcategory_dim] (

	[SubcategoryName] varchar(8000) NULL, 
	[ProductSubcategoryId] int NULL, 
	[ProductCategoryId] int NULL, 
	[valid_from_dttm] datetime2(6) NULL, 
	[valid_to_dttm] datetime2(6) NULL, 
	[processed_dttm] datetime2(6) NULL, 
	[product_subcategory_rk] int NULL
);