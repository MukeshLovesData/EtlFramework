CREATE TABLE [DW_WH_ControlDB].[dbo].[ETLLoads](
	[LoadID] [int] NOT NULL,
	[SourceZone] [varchar](30) NOT NULL,
	[SinkZone] [varchar](30) NOT NULL,
	[SourceName] [varchar](20) NOT NULL,    
	[SourceType] [varchar](20) NOT NULL,
	[SinkType] [varchar](20) NOT NULL,    --
	[SourceEntityName] [varchar](100) NOT NULL, --
    [SinkEntityName] [varchar](100) NOT NULL, --
	[LoadType] [varchar](12) NOT NULL,
	[LoadGroup] [varchar](12) NULL,
	[Disabled] [bit] NULL,
	[RowCount] [bigint] NULL,
	[PipelineEndTime] [datetime2](6) NULL
) 
GO
