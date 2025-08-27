CREATE TABLE [dwh].[territory_dim] (

	[Region] varchar(8000) NULL, 
	[Country] varchar(8000) NULL, 
	[Continent] varchar(8000) NULL, 
	[TerritoryId] int NULL, 
	[valid_from_dttm] datetime2(6) NULL, 
	[valid_to_dttm] datetime2(6) NULL, 
	[processed_dttm] datetime2(6) NULL, 
	[territory_rk] int NULL
);