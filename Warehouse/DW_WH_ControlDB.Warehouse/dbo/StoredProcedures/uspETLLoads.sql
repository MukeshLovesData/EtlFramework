CREATE PROCEDURE [dbo].[uspETLLoads]
	 @LoadGroup	NVARCHAR(5)	= N'ALL',
	 @LoadType	NVARCHAR(12)	= N'ALL'
AS 

BEGIN
    SET @LoadGroup = ISNULL(@LoadGroup, 'ALL');
    SET @LoadType = ISNULL(@LoadType, 'ALL');

	SELECT
		l.SourceZone,
        l.SinkZone,
        l.SourceName, 
        l.SourceType,
        l.SinkType,
		l.SourceEntityName,
        l.SinkEntityName, 
		l.LoadType
	FROM dbo.ETLLoads AS l
	WHERE
		l.Disabled = 0
		AND (@LoadGroup = 'ALL' OR l.LoadGroup = @LoadGroup)
		AND (@LoadType = 'ALL' OR l.LoadType = @LoadType)
END