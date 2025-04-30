CREATE TABLE [dbo].[dim_date] (
   [sk_date] [int] NOT NULL,
   [date] [date] NOT NULL,
   [day] [int] NOT NULL,
   [day_suffix] [char](2) NOT NULL,
   [week_day] [int] NOT NULL,
   [week_day_name] [varchar](10) NOT NULL,
   [week_day_name_short] [char](3) NOT NULL,
   [week_day_name_first_letter] [char](1) NOT NULL,
   [DOW_in_month] [int] NOT NULL,
   [day_of_year] [smallint] NOT NULL,
   [week_of_month] [int] NOT NULL,
   [week_of_year] [int] NOT NULL,
   [month] [int] NOT NULL,
   [month_name] [varchar](10) NOT NULL,
   [month_name_short] [char](3) NOT NULL,
   [month_name_first_letter] [char](1) NOT NULL,
   [quarter] [int] NOT NULL,
   [quarter_name] [varchar](6) NOT NULL,
   [year] [int] NOT NULL,
   [MMYYYY] [char](6) NOT NULL,
   [month_year] [char](7) NOT NULL,
   [is_weekend] BIT NOT NULL,
   [is_holiday] BIT NOT NULL,
   [holiday_name] VARCHAR(20) NULL,
   [special_days] VARCHAR(20) NULL,
   [financial_year] [int] NULL,
   [financial_quarter] [int] NULL,
   [financial_month] [int] NULL,
   [first_date_of_year] DATE NULL,
   [last_date_of_year] DATE NULL,
   [first_date_of_quarter] DATE NULL,
   [last_date_of_quarter] DATE NULL,
   [first_date_of_month] DATE NULL,
   [last_date_of_month] DATE NULL,
   [first_date_of_week] DATE NULL,
   [last_date_of_week] DATE NULL,
   [current_year] SMALLINT NULL,
   [current_quarter] SMALLINT NULL,
   [current_month] SMALLINT NULL,
   [current_week] SMALLINT NULL,
   [current_day] SMALLINT NULL
   );

SET NOCOUNT ON

SET DATEFIRST 1;
DECLARE @CurrentDate DATE = '2022-01-01'
DECLARE @EndDate DATE = '2026-12-31'

WHILE @CurrentDate < @EndDate
BEGIN
   INSERT INTO [dbo].[dim_date] (
      [sk_date],
      [date],
      [day],
      [day_suffix],
      [week_day],
      [week_day_name],
      [week_day_name_short],
      [week_day_name_first_letter],
      [DOW_in_month],
      [day_of_year],
      [week_of_month],
      [week_of_year],
      [month],
      [month_name],
      [month_name_short],
      [month_name_first_letter],
      [quarter],
      [quarter_name],
      [year],
      [MMYYYY],
      [month_year],
      [is_weekend],
      [is_holiday],
      [first_date_of_year],
      [last_date_of_year],
      [first_date_of_quarter],
      [last_date_of_quarter],
      [first_date_of_month],
      [last_date_of_month],
      [first_date_of_week],
      [last_date_of_week]
      )
   SELECT [sk_date] = YEAR(@CurrentDate) * 10000 + MONTH(@CurrentDate) * 100 + DAY(@CurrentDate),
      [date] = @CurrentDate,
      [date] = DAY(@CurrentDate),
      [day_suffix] = CASE 
         WHEN DAY(@CurrentDate) = 1
            OR DAY(@CurrentDate) = 21
            OR DAY(@CurrentDate) = 31
            THEN 'st'
         WHEN DAY(@CurrentDate) = 2
            OR DAY(@CurrentDate) = 22
            THEN 'nd'
         WHEN DAY(@CurrentDate) = 3
            OR DAY(@CurrentDate) = 23
            THEN 'rd'
         ELSE 'th'
         END,
--      [week_day] = DATEPART(dw, @CurrentDate), -- by default, Sunday is week_day = 1. At BK Scandi, start week is Monday so use below instead
      [week_day] = DATEPART(dw, DATEADD(day,-1,@CurrentDate)),
      [week_day_name] = DATENAME(dw, @CurrentDate),
      [week_day_name_short] = UPPER(LEFT(DATENAME(dw, @CurrentDate), 3)),
      [week_day_name_first_letter] = LEFT(DATENAME(dw, @CurrentDate), 1),
      [DOW_in_month] = DAY(@CurrentDate),
      [day_of_year] = DATENAME(dy, @CurrentDate),
      [week_of_month] = DATEPART(WEEK, @CurrentDate) - DATEPART(WEEK, DATEADD(MM, DATEDIFF(MM, 0, @CurrentDate), 0)) + 1,
      [week_of_year] = DATEPART(ISO_WEEK, @CurrentDate),
      [month] = MONTH(@CurrentDate),
      [month_name] = DATENAME(mm, @CurrentDate),
      [month_name_short] = UPPER(LEFT(DATENAME(mm, @CurrentDate), 3)),
      [month_name_first_letter] = LEFT(DATENAME(mm, @CurrentDate), 1),
      [quarter] = DATEPART(q, @CurrentDate),
      [quarter_name] = CASE 
         WHEN DATENAME(qq, @CurrentDate) = 1
            THEN 'First'
         WHEN DATENAME(qq, @CurrentDate) = 2
            THEN 'second'
         WHEN DATENAME(qq, @CurrentDate) = 3
            THEN 'third'
         WHEN DATENAME(qq, @CurrentDate) = 4
            THEN 'fourth'
         END,
      [year] = YEAR(@CurrentDate),
      [MMYYYY] = RIGHT('0' + CAST(MONTH(@CurrentDate) AS VARCHAR(2)), 2) + CAST(YEAR(@CurrentDate) AS VARCHAR(4)),
      [month_year] = CAST(YEAR(@CurrentDate) AS VARCHAR(4)) + UPPER(LEFT(DATENAME(mm, @CurrentDate), 3)),
      [is_weekend] = CASE 
   WHEN (DATEPART(dw, @CurrentDate) + @@DATEFIRST - 1) % 7 + 1 IN (6, 7)
      THEN 1
   ELSE 0
   END,
      [is_holiday] = 0,
      [first_date_of_week] = DATEADD(DAY, -((DATEPART(dw, @CurrentDate) + @@DATEFIRST - 2) % 7), @CurrentDate),
      [last_date_of_week] = DATEADD(DAY, 6 - ((DATEPART(dw, @CurrentDate) + @@DATEFIRST - 2) % 7), @CurrentDate),
      [first_date_of_quarter] = DATEADD(qq, DATEDIFF(qq, 0, GETDATE()), 0),
      [last_date_of_quarter] = DATEADD(dd, - 1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) + 1, 0)),
      [first_date_of_month] = CAST(CAST(YEAR(@CurrentDate) AS VARCHAR(4)) + '-' + CAST(MONTH(@CurrentDate) AS VARCHAR(2)) + '-01' AS DATE),
      [last_date_of_month] = EOMONTH(@CurrentDate),
      [first_date_of_week] = DATEADD(dd, - (DATEPART(dw, @CurrentDate) - 1), @CurrentDate),
      [last_date_of_week] = DATEADD(dd, 7 - (DATEPART(dw, @CurrentDate)), @CurrentDate)

   SET @CurrentDate = DATEADD(DD, 1, @CurrentDate)
END

--Update Holiday information
UPDATE dim_date
SET [is_holiday] = 1,
   [holiday_name] = 'Christmas'
WHERE [month] = 12
   AND [day] = 25

UPDATE dim_date
SET [special_days] = 'Valentines Day'
WHERE [month] = 2
   AND [day] = 14

--Update current date information
UPDATE dim_date
SET [current_year] = DATEDIFF(yy, GETDATE(), DATE),
    [current_quarter] = DATEDIFF(q, GETDATE(), DATE),
    [current_month] = DATEDIFF(m, GETDATE(), DATE),
    [current_week] = DATEDIFF(ww, GETDATE(), DATE),
    [current_day] = DATEDIFF(dd, GETDATE(), DATE)