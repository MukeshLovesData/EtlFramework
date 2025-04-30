CREATE TABLE [dbo].[dimTime] AS

WITH E00(N) AS (SELECT 1 UNION ALL SELECT 1)
    ,E02(N) AS (SELECT 1 FROM E00 a, E00 b)
    ,E04(N) AS (SELECT 1 FROM E02 a, E02 b)
    ,E08(N) AS (SELECT 1 FROM E04 a, E04 b)
    ,E16(N) AS (SELECT 1 FROM E08 a, E08 b)
    ,E32(N) AS (SELECT 1 FROM E16 a, E16 b)
	-- Above ctes create a table with the max no of rows allowed by the integer data type
    ,cteTally(N) AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM E32) -- Uses row number to convert to a list of all possible numbers allowable in int
	,cteTimes(TimeStmp) AS (SELECT CAST(DATEADD(SECOND,N - 1,'2020-01-01') AS TIME(0)) FROM cteTally WHERE N <= 86400) -- Uses number list to create one row per second
	,cteDimTime AS (
		SELECT 
			1000000 + DATEPART(HOUR, t.TimeStmp) * 10000 + DATEPART(MINUTE, t.TimeStmp) * 100 + DATEPART(SECOND, t.TimeStmp) AS TimeKey
			, t.TimeStmp AS Time
			, DATEPART(HOUR, t.TimeStmp) AS HourNo
			, DATEPART(MINUTE, t.TimeStmp) AS MinuteNo
			, DATEPART(SECOND, t.TimeStmp) AS SecondNo
			, CAST(DATEADD(MINUTE, FLOOR(DATEDIFF(MINUTE, 0, t.TimeStmp) / 15.0) * 15, 0) AS TIME(0)) AS FifteenMinFrom
			, CAST(DATEADD(SECOND, 899, DATEADD(MINUTE, FLOOR(DATEDIFF(MINUTE, 0, t.TimeStmp) / 15.0) * 15, 0)) AS TIME(0)) AS FifteenMinTo
			, CAST(DATEADD(MINUTE, FLOOR(DATEDIFF(MINUTE, 0, t.TimeStmp) / 30.0) * 30, 0) AS TIME(0)) AS ThirtyMinFrom
			, CAST(DATEADD(SECOND, 1799, DATEADD(MINUTE, FLOOR(DATEDIFF(MINUTE, 0, t.TimeStmp) / 30.0) * 30, 0)) AS TIME(0)) AS ThirtyMinTo
			, CASE WHEN t.TimeStmp BETWEEN '06:00' AND '11:59:59' THEN 1
				   WHEN t.TimeStmp BETWEEN '12:00' AND '16:59:59' THEN 2
				   WHEN t.TimeStmp BETWEEN '17:00' AND '21:59:59' THEN 3
				   ELSE 4
			  END AS DayPartID
			, CASE WHEN t.TimeStmp BETWEEN '06:00' AND '11:59:59' THEN N'Brunch'
				   WHEN t.TimeStmp BETWEEN '12:00' AND '16:59:59' THEN N'Daytime'
				   WHEN t.TimeStmp BETWEEN '17:00' AND '21:59:59' THEN N'Evening'
				   ELSE 'Late'
			  END AS DayPartName
		FROM cteTimes AS t
	)

	SELECT dt.TimeKey
         , dt.Time
         , dt.HourNo
         , dt.MinuteNo
         , dt.SecondNo
		 , CONVERT(VARCHAR(8), dt.FifteenMinFrom, 8) + N' to ' + CONVERT(VARCHAR(8), dt.FifteenMinTo, 8) AS FifteenMinBucket
         , dt.FifteenMinFrom
         , dt.FifteenMinTo
		 , CONVERT(VARCHAR(8), dt.ThirtyMinFrom, 8) + N' to ' + CONVERT(VARCHAR(8), dt.ThirtyMinTo, 8) AS ThirtyMinBucket
         , dt.ThirtyMinFrom
         , dt.ThirtyMinTo
         , dt.DayPartID
         , dt.DayPartName
		 ,0 AS DeletedInSource
		 ,dt.TimeKey AS KeyCol
	FROM cteDimTime AS dt

GO