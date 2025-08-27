CREATE TABLE [dwh].[customer_dim] (

	[Prefix] varchar(8000) NULL, 
	[FirstName] varchar(8000) NULL, 
	[LastName] varchar(8000) NULL, 
	[BirthDate] varchar(8000) NULL, 
	[MaritalStatus] varchar(8000) NULL, 
	[Gender] varchar(8000) NULL, 
	[EmailAddress] varchar(8000) NULL, 
	[AnnualIncome] varchar(8000) NULL, 
	[EducationLevel] varchar(8000) NULL, 
	[Occupation] varchar(8000) NULL, 
	[HomeOwner] varchar(8000) NULL, 
	[valid_from_dttm] datetime2(6) NULL, 
	[valid_to_dttm] datetime2(6) NULL, 
	[processed_dttm] datetime2(6) NULL, 
	[customer_rk] int NULL, 
	[CustomerId] int NULL, 
	[TotalChildren] int NULL
);