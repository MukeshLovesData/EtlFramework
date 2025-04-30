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
server = ''
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
load_type = 'sss'

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
df_entity = spark.sql(eval(f"\"select * from DE_LH_CONTROL.ETLLoads where LoadGroup = '{load_group}' and LoadType = '{load_type}' and Disabled = 0\""))
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
    if load_type == "sss":

        print(source_zone)
        spark.sql(f"use {source_zone}")
        
        # Split the SQL script into individual statements
        sql_statements = str_sql_script.split(';')

        # Execute each statement separately
        for statement in sql_statements:
            # Clean the statement (remove empty lines and leading/trailing whitespace)
            clean_statement = statement.strip()
            if clean_statement:  # Only execute non-empty statements
                try:
                    # Execute the statement
                    spark.sql(clean_statement)
                    print(f"Successfully executed: {clean_statement[:100]}...") # Print first 100 chars for logging
                except Exception as e:
                    print(f"Error executing statement: {clean_statement[:100]}...")
                    print(f"Error message: {str(e)}")
                    raise  # Re-raise the exception if you want to stop execution on error
    else:
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

 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
