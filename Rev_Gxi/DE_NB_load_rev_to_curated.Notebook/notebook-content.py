# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "17eb4c2e-c36a-4a32-a629-073e10542b56",
# META       "default_lakehouse_name": "DE_LH_400_CURATED",
# META       "default_lakehouse_workspace_id": "809b2e69-e0f0-4edd-b871-3be52fbf6a09",
# META       "known_lakehouses": [
# META         {
# META           "id": "17eb4c2e-c36a-4a32-a629-073e10542b56"
# META         },
# META         {
# META           "id": "247c8ebb-ebc0-4b3d-87ad-85bce0ebbf1f"
# META         },
# META         {
# META           "id": "0ef699fb-9a0c-4215-b106-b4241d67e305"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# set up notebook
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.context import SparkContext

import datetime


# Create a Spark session with case sensitivity enabled
spark = SparkSession.builder \
    .appName("RevGXI") \
    .config("spark.sql.caseSensitive", "true") \
    .getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the SQL query
query = """
SELECT  s.SiteNumber AS SiteNumber,
    s.Country AS Country,
    `Address`,
    `Sub-Franchisee`,
    `Employees`,
    `Total_Courses`,
    `Completed`,
    b.`%_Not_Started`,
    b.`%_In_Progress`,
    b.`%_of_Completion`,
    CAST(date_format(b.LoadDateTime, 'yyyy-MM-dd') AS DATE) AS `Date`,
    b.FileName
    ,b.ConvertedDate
    ,md5(CONCAT(b.FileName,s.SiteNumber ,CAST(b.ConvertedDate AS STRING))) AS HashKey
FROM BKU b 
LEFT JOIN sharepoint_DimSiteSupplement s 
ON b.Restaurant_ID = s.RBISiteNumber
"""
spark.sql("use DE_LH_100_RAW")
df = spark.sql(query)
df.createOrReplaceTempView("vwBKU")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


##add SiteSks 
siteSkQuery="""select 
            b.sk_location
            ,a.Country,
            `Address`,
            `Sub-Franchisee`,
            `Employees`,
            `Total_Courses`,
            `Completed`,
            a.`%_Not_Started`,
            a.`%_In_Progress`,
            a.`%_of_Completion`,
            a.`Date`,
            a.FileName
            ,a.HashKey
            ,a.ConvertedDate
            from vwBKU a 
            INNER join
            DE_LH_400_CURATED.dim_location b
            on a.SiteNumber = b.location_id and a.Country = b.country"""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the dashboard query
dashboardQuery = """
SELECT  s.SiteNumber AS SiteNumber,
    s.Country AS Country,
     `Restaurant`
            ,`GXI`
            ,`OSAT`
            ,`Number_of_Responses`
            ,`Google_Rating`
            ,`Number_of_Google_Reviews`
            ,`SOS`
            ,`PSAT`
            ,`Experienced_Problem?`
            ,CAST(date_format(b.LoadDateTime, 'yyyy-MM-dd') AS DATE) AS `Date`
            ,b.FileName
            ,b.ConvertedDate
            ,md5(CONCAT(b.FileName,s.SiteNumber ,CAST(b.ConvertedDate AS STRING))) AS HashKey
FROM dashboard_export b 
    LEFT join sharepoint_DimSiteSupplement s 
    on b.Restaurant = s.RBISiteNumber
"""
spark.sql("use DE_LH_100_RAW")
df = spark.sql(dashboardQuery)
df.createOrReplaceTempView("vwDashboard")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##add SiteSks 
siteSkDashboardQuery="""select 
            b.sk_location
            ,a.Country
            ,`Restaurant`
            ,`GXI`
            ,`OSAT`
            ,`Number_of_Responses`
            ,`Google_Rating`
            ,`Number_of_Google_Reviews`
            ,`SOS`
            ,`PSAT`
            ,`Experienced_Problem?`
            ,a.`Date`
            ,a.FileName
            ,a.HashKey
            ,a.ConvertedDate
            from vwDashboard a 
            INNER join
            DE_LH_400_CURATED.dim_location b
            on a.SiteNumber = b.location_id and a.Country = b.country"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the OrdinaryAudit query
auditQuery = """
        SELECT 
            s.SiteNumber AS SiteNumber, 
            s.Country AS Country,
            `ID`
            ,`BRAND`
            ,`ROUND`
            ,`REGION`
            ,`FRANCHISE`
            ,`LOCATION`
            ,`LOCATION_NAME`
            ,`ROUTE`
            ,`VISIT_TYPE`
            ,`SUBTYPE`
            ,`VISIT_STATUS`
            ,`STATUS_ON_SYSTEM`
            ,`NOTIFICATION`
            ,`VISIT_DATE`
            ,`DAY_PART`
            ,`UPLOADED_DATE`
            ,`RELEASE_DATE`
            ,`WEEK_DAY`
            ,`AUDITOR_NAME`
            ,`REVISION_TIME`
            ,`DELIVERY_TIME`
            ,`ZERO_-_CONDITION`
            ,`ZERO_-_COMMENTS`
            ,`OPERATIONS_STDS__%_`
            ,`OPERATIONS_STDS__TARGET_PTS_`
            ,`FOOD_SAFETY__PTS_`
            ,`FOOD_SAFETY__TARGET_PTS_`
            ,`FOOD_SAFETY__CRITICAL_PTS_`
            ,`FOOD_SAFETY__IMPORTANT_PTS_`
            ,`_#__FS_Escalations`
            ,`_#__Red_Flags`
            ,`GECs__PTS_`
            ,`R.I.`
            ,`R.I.__PTS_`
            ,`OPERATIONS_STD_GRADE`
            ,`FOOD_SAFETY_GRADE`
            ,`_%__Guest_Service_and_Digital_OPS`
            ,`Guest_Service_and_Digital_OPS__TARGET_PTS_`
            ,`_%__Food_Quality`
            ,`Food_Quality__TARGET_PTS_`
            ,`_%__Management`
            ,`Management__TARGET_PTS_`
            ,`_%__Guest_Journey_-_Cleanliness_and_MaintenanceOPS`
            ,`Guest_Journey_-_Cleanliness_and_MaintenanceOPS__TARGET_PTS_`
            ,`_%__Back_of_House`
            ,`Back_of_House__TARGET_PTS_`
            ,`FC`
            ,`DT`
            ,`WIN`
            ,`Reviewed_By`
            ,`LoadDateTime`
            ,`FileName`
            ,`ConvertedDate`
            ,md5(CONCAT(b.FileName,b.ID,b.SUBTYPE,s.SiteNumber ,CAST(b.ConvertedDate AS STRING))) AS HashKey
FROM OrdinaryAudit b 
    LEFT join sharepoint_DimSiteSupplement s 
    on b.LOCATION = s.RBISiteNumber
WHERE s.SiteNumber is not null
"""
spark.sql("use DE_LH_100_RAW")
df = spark.sql(auditQuery)
df.createOrReplaceTempView("vwAudit")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##add SiteSks 
siteSkAuditQuery="""select 
            b.sk_location
            ,a.*
            from vwAudit a 
            INNER join
            DE_LH_400_CURATED.dim_location b
            on a.SiteNumber = b.location_id and a.Country = b.country"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  a list of dictionaries
ls = [{'TableName': 'bkuData', 'Query': siteSkQuery},{'TableName': 'OrdinaryAuditData', 'Query': siteSkAuditQuery}, {'TableName': 'dashboardExportData', 'Query': siteSkDashboardQuery}]


# Iterating over the list 
for row in ls:
    spark.sql("use DE_LH_100_RAW")
    df = spark.sql(row['Query'])
    df.createOrReplaceTempView("vwOutpot")
    temp_view_df = spark.sql(" SELECT * FROM vwOutpot")

    deltaPath = f"abfss://809b2e69-e0f0-4edd-b871-3be52fbf6a09@onelake.dfs.fabric.microsoft.com/17eb4c2e-c36a-4a32-a629-073e10542b56/Tables/{row['TableName']}"

    if not DeltaTable.isDeltaTable(spark, deltaPath):
        df.write \
            .mode("overwrite") \
            .format("delta") \
            .option("mergeSchema", "true")\
            .saveAsTable(f"DE_LH_400_CURATED.{row['TableName']}")
            
    else:
        print("Else")
        delta_table = DeltaTable.forPath(spark, deltaPath)
        delta_table.alias("tgt").merge(
        temp_view_df.alias("src"),
        "tgt.HashKey = src.HashKey"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
