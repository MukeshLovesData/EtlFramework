# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "93c27ff5-bb90-4ca3-b7d6-887b162b66b4",
# META       "default_lakehouse_name": "DE_LH_CONTROL",
# META       "default_lakehouse_workspace_id": "809b2e69-e0f0-4edd-b871-3be52fbf6a09",
# META       "known_lakehouses": [
# META         {
# META           "id": "93c27ff5-bb90-4ca3-b7d6-887b162b66b4"
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

import requests
import json
import pyodbc
import os
import datetime

# connect to Control DB (required for write)
key_vault = 'https://kv-bks-dev-neu-001.vault.azure.net/'
driver= '{ODBC Driver 18 for SQL Server}'
#server = mssparkutils.credentials.getSecret(key_vault,'fabric-control-db-server')
server = 'qnevyyf2jhmu7h7c6l5skmjzji-nexjxahq4dou5odrhpss7p3kbe.datawarehouse.fabric.microsoft.com'
database = 'DW_WH_ControlDB'
fabric_client_id = mssparkutils.credentials.getSecret(key_vault,'fabric-client-id')
fabric_client_secret = mssparkutils.credentials.getSecret(key_vault,'fabric-client-secret')
authentication = 'ActiveDirectoryServicePrincipal'

# Create a Spark session with case sensitivity enabled
spark = SparkSession.builder \
    .appName("CaseSensitiveSQL") \
    .config("spark.sql.caseSensitive", "true") \
    .getOrCreate()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

load_group = 'load'
load_type = 'agg'
pipeline_runid = 'manual run DE_NB_DeltaToDelta'
SinkEntityName = 'fact_sales_merged_item'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''
to do: use RDD and compare time with sequential run
- when deduping dim: run 10 entities in parallel (maybe 7 max?)
- when deduping fact: run 3 entities in parallel
''' 

# load Data from SourceZone to SinkZone

# connect to Control DB (required for write)
#conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+fabric_client_id+';PWD='+fabric_client_secret+';Authentication='+authentication)

# get rows from ControlDB
df_entity = spark.sql(eval(f"\"select * from DE_LH_CONTROL.ETLLoads where LoadGroup = '{load_group}' and LoadType = '{load_type}' and SinkEntityName = 'fact_sales_merged_item' and Disabled = 0\""))
ls_entity = df_entity.collect()

# loop through records
for row in ls_entity:
    source_name = row.SourceName
    source_zone = row.SourceZone
    sink_zone = row.SinkZone
    entity_name = row.SinkEntityName
    load_type = row.LoadType

    print(entity_name)
    
    # read SQL script from storage
    ls_sql_script = spark.read.text(f"Files/{load_group}/{entity_name}.txt").collect()
    # exclude comment lines
    df_sql_script = spark.createDataFrame(ls_sql_script, ["value"])
    df_sql_no_comments = df_sql_script.filter(~df_sql_script.value.contains("--"))
    ls_sql_no_comments = df_sql_no_comments.collect()
    # convert the script content into a single string
    str_sql_script = " ".join([row["value"] for row in ls_sql_no_comments])

    # run SQL
    spark.sql(f"use {source_zone}")
    df = spark.sql(eval(f"\"{str_sql_script}\""))
    
    # add meta data columns
    df = df.withColumn("dw_source", lit(f"{source_name}")) \
            .withColumn("dw_pipeline_runid", lit(f"{pipeline_runid}")) \
            .withColumn("dw_loaded_at", current_timestamp()) 

    # write to SinkZone table
    df.write \
        .mode("overwrite") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{sink_zone}.{entity_name}")

    # log max fact dates
    if load_group == 'dedup' and load_type == 'fact':

        '''
        ls_sql_script = spark.read.text(f"Files/fact_dates/{entity_name}.txt").collect()
        # exclude comment lines
        df_sql_script = spark.createDataFrame(ls_sql_script, ["value"])
        df_sql_no_comments = df_sql_script.filter(~df_sql_script.value.contains("--"))
        ls_sql_no_comments = df_sql_no_comments.collect()
        # convert the script content into a single string
        str_sql_script = " ".join([row["value"] for row in ls_sql_no_comments])
        '''
        
        str_sql_script = 'select max(transaction_closed_date) as max_date from guest_checks'

        # run SQL
        spark.sql(f"use {sink_zone}")
        df_max_date = spark.sql(eval(f"\"{str_sql_script}\""))
        df_max_date = df_max_date.withColumn("run_at", current_timestamp()) 

        df_max_date.write \
        .mode("overwrite") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"DE_LH_CONTROL.ETL_LastStagedDates")
        #.saveAsTable(f"DE_LH_CONTROL.ETL_LastStagedDates_{entity_name}")

    '''    
    # update ControlDB
    rowCount = str(df.count())
    cursor = conn.cursor()
    sql_update_query = """
    UPDATE [dbo].[ETLLoads]
    SET [RowCount] = """ + rowCount + """
    ,[PipelineEndTime] = GetDate()
    WHERE [SinkEntityName] = '""" + entity_name + """';   
    """
    cursor.execute(sql_update_query)
    conn.commit()
    '''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
