# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "247c8ebb-ebc0-4b3d-87ad-85bce0ebbf1f",
# META       "default_lakehouse_name": "DE_LH_100_RAW",
# META       "default_lakehouse_workspace_id": "809b2e69-e0f0-4edd-b871-3be52fbf6a09",
# META       "known_lakehouses": [
# META         {
# META           "id": "247c8ebb-ebc0-4b3d-87ad-85bce0ebbf1f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import os
import shutil

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

load_group = 'flatt'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get list of Entities from ETLLoads
df_entity = spark.sql(eval(f"\"select SinkEntityName from DE_LH_CONTROL.ETLLoads where LoadGroup = '{load_group}' and SinkEntityName = 'kdsDetails' and Disabled = 0\""))
ls_entity = [row[0] for row in df_entity.select("SinkEntityName").collect()]
print(ls_entity)

#spark = SparkSession.builder.appName("RunParallelJobs").getOrCreate()
entity_rdd = spark.sparkContext.parallelize(ls_entity)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get list of Entities from ETLLoads
df_entity = spark.sql(eval(f"\"select SinkEntityName from DE_LH_CONTROL.ETLLoads where LoadGroup = '{load_group}' and Disabled = 0\""))
ls_entity = [row[0] for row in df_entity.select("SinkEntityName").collect()]
print(ls_entity)

#spark = SparkSession.builder.appName("RunParallelJobs").getOrCreate()
entity_rdd = spark.sparkContext.parallelize(ls_entity)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# udf to be applied to each entity in RDD

def archive_files(entity):

    # get data
    print(entity)

    # Define source and target directories
    source_dir = f'//lakehouse/default/Files/{entity}/'
    target_dir = f'//lakehouse/default/Files/{entity}/processed/'

    # Ensure target directory exists
    os.makedirs(target_dir, exist_ok=True)

    # List all files in the source directory
    ls_files = os.listdir(source_dir)
    ls_files = [item for item in ls_files if item != 'processed']

    # Move each file to the target directory
    for file_name in ls_files:
        source_file = os.path.join(source_dir, file_name)
        target_file = os.path.join(target_dir, file_name)
        shutil.move(source_file, target_file)
    
    return 'archived'

# archive each entity in the RDD in parallel
response_rdd = entity_rdd.map(archive_files)
results = response_rdd.collect()
print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for row in ls_entity:
    print(row)

    # Define source and target directories
    source_dir = f'//lakehouse/default/Files/{row}/'
    target_dir = f'//lakehouse/default/Files/{row}/processed/'

    # Ensure target directory exists
    os.makedirs(target_dir, exist_ok=True)

    # List all files in the source directory
    ls_files = os.listdir(source_dir)
    ls_files = [item for item in ls_files if item != 'processed']

    # Move each file to the target directory
    for file_name in ls_files:
        source_file = os.path.join(source_dir, file_name)
        target_file = os.path.join(target_dir, file_name)
        shutil.move(source_file, target_file)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
