CREATE TABLE [dbo].[DimAccount] (

	[DimAccountSk] int NULL, 
	[DimAccountRk] int NULL, 
	[AccountId] int NULL, 
	[MainBranch] varchar(100) NULL, 
	[Country] varchar(100) NULL, 
	[AccountNumber] int NULL, 
	[AccountName] varchar(100) NULL, 
	[AccountType] varchar(100) NULL, 
	[AccountCategory] varchar(100) NULL, 
	[Description] varchar(100) NULL, 
	[AccountLevel1] varchar(100) NULL, 
	[AccountLevel2] varchar(100) NULL, 
	[AccountLevel1ID] int NULL, 
	[AccountLevel2ID] int NULL, 
	[FinancialStatement] varchar(100) NULL, 
	[IsAggregated] int NULL, 
	[ValidFrom] date NULL, 
	[ValidTo] date NULL, 
	[Active] bit NULL, 
	[LoadedDateTime] date NULL, 
	[RunId] varchar(100) NULL
);