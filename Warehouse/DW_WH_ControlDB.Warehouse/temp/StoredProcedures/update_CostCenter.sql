CREATE PROC [temp].[update_CostCenter] AS

IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'temp.DimCostCenter')
)

    BEGIN
	    TRUNCATE TABLE [temp].[DimCostCenter]
    END

ELSE
    BEGIN
        CREATE TABLE [temp].[DimCostCenter]
        (
			[Dimensions] [varchar](100) NULL,
			[CostCenterID] [int] NULL,
			[MainBranch] [varchar](100) NULL,
			[Country] [varchar](100) NULL,
			[CostCenterNumber] [int] NULL,
			[CostCenterName] [varchar](100) NULL
        )
        
    END

BEGIN

	INSERT INTO [temp].[DimCostCenter]
	
	SELECT 
	   [Dimensions]
      ,[CostCenterID]
      ,[MainBranch]
      ,[Country]
      ,[CostCenterNumber]
      ,[CostCenterName]
  FROM [stage].[Azets_CostCenterNO]
  
	UNION

	SELECT 
	   [Dimensions]
      ,[CostCenterID]
      ,[MainBranch]
      ,[Country]
      ,[CostCenterNumber]
      ,[CostCenterName]
  FROM [stage].[Azets_CostCenterSE]

	UNION

	SELECT 
	   [Dimensions]
      ,[CostCenterID]
      ,[MainBranch]
      ,[Country]
      ,[CostCenterNumber]
      ,[CostCenterName]
  FROM [stage].[Azets_CostCenterDK]

 END