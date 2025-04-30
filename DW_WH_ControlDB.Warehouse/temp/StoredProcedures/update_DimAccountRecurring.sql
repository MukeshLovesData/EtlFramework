CREATE PROC [temp].[update_DimAccountRecurring] AS

IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'temp.DimAccountRecurring')
)
    BEGIN
	    TRUNCATE TABLE [temp].[DimAccountRecurring]
    END
ELSE
    BEGIN
        CREATE TABLE [temp].[DimAccountRecurring]
        (
	        [AccountId] [int] NULL,
	        [MainBranch] [varchar](100) NULL,
	        [Country] [varchar](100) NULL,
	        [AccountNumber] [int] NULL,
	        [AccountName] [varchar](100) NULL,
	        [AccountType] [varchar](100) NULL,
	        [AccountCategory] [varchar](100) NULL,
	        [Description] [varchar](100) NULL,
	        [Accountlevel1] [varchar](100) NULL,
	        [Accountlevel2] [varchar](100) NULL,
	        [Accountlevel1ID] [int] NULL,
	        [Accountlevel2ID] [int] NULL,
	        [FinancialStatement] [varchar](100) NULL
        )
       
    END

BEGIN
	INSERT INTO temp.DimAccountRecurring
	
	SELECT 
	   [AccountId]
      ,[MainBranch]
      ,TRIM([Country]) AS Country
      ,[AccountNumber]
      ,[AccoutName] AS AccountName
      ,[AccountType]
      ,[AccountCategory]
      ,[Description]
    ,[Accountlevel1],
			[Accountlevel2],
			[Accountlevel1ID],
			[Accountlevel2ID],
			[Financialstatement]
	FROM [stage].Azets_AccountNO

	UNION

	SELECT 
	   [AccountId]
      ,[MainBranch]
      ,TRIM([Country]) AS Country
      ,[AccountNumber]
      ,[AccoutName] AS AccountName
      ,[AccountType]
      ,[AccountCategory]
      ,[Description]
    ,[Accountlevel1],
			[Accountlevel2],
			[Accountlevel1ID],
			[Accountlevel2ID],
			[Financialstatement]
	FROM [stage].Azets_AccountSE

    UNION

	SELECT 
	   [AccountId]
      ,[MainBranch]
      ,TRIM([Country]) AS Country
      ,[AccountNumber]
      ,[AccountName]
      ,[AccountType]
      ,[AccountCategory]
      ,[Description]
      ,	[AccountLevel1],
			[AccountLevel2],
			[AccountLevel1ID],
			[AccountLevel2ID],
			[FinancialStatement]
	FROM [stage].Azets_AccountDK

 END