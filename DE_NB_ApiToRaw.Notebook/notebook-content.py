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
# META         },
# META         {
# META           "id": "93c27ff5-bb90-4ca3-b7d6-887b162b66b4"
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
from datetime import datetime, timedelta

# get Oracle API variables
key_vault = 'https://kv-bks-dev-neu-001.vault.azure.net/'
Oracle_AuthURL = mssparkutils.credentials.getSecret(key_vault,'Oracle-AuthURL')
Oracle_BaseURL = mssparkutils.credentials.getSecret(key_vault,'Oracle-BaseURL')
Oracle_ClientID = mssparkutils.credentials.getSecret(key_vault,'Oracle-ClientID')
Oracle_APIUsername = mssparkutils.credentials.getSecret(key_vault,'Oracle-APIUsername')
Oracle_APIPassword = mssparkutils.credentials.getSecret(key_vault,'Oracle-APIPassword')
Oracle_APIOrgName = mssparkutils.credentials.getSecret(key_vault,'Oracle-APIOrgName')
Oracle_CodeVerifier = mssparkutils.credentials.getSecret(key_vault,'Oracle-CodeVerifier')
Oracle_CodeChallenge = mssparkutils.credentials.getSecret(key_vault,'Oracle-CodeChallenge')
Oracle_DefaultLocRef = mssparkutils.credentials.getSecret(key_vault,'Oracle-DefaultLocRef')
Oracle_AllLocRef = mssparkutils.credentials.getSecret(key_vault,'Oracle-AllLocRef')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# get pipeline parameters

load_group = 'stage'

guestChecks_start_date = '2025-01-23'
guestChecks_end_date = '2025-01-23'

guestChecks_off_till_start_date = '2025-01-23'
guestChecks_off_till_end_date = '2025-01-23'
guestChecks_off_till_changed_since_datetime = '2025-01-23T00:00:00Z'

menuItemDailyTotals_start_date = '2025-01-23'
menuItemDailyTotals_end_date = '2025-01-23'

kdsDetails_start_date = '2025-01-23'
kdsDetails_end_date = '2025-01-23'

locRef = 'all'
workspace_id = '809b2e69-e0f0-4edd-b871-3be52fbf6a09'
de_lh_control_id = '93c27ff5-bb90-4ca3-b7d6-887b162b66b4'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# authentication Workflow

# 1. authorize
def authorize():
    url = f"{Oracle_AuthURL}/oidc-provider/v1/oauth2/authorize?response_type=code&client_id={Oracle_ClientID}&scope=openid&redirect_uri=apiaccount://callback&code_challenge={Oracle_CodeChallenge}&code_challenge_method=S256"

    payload = {}
    headers = {
        'Cookie': f"client_id={Oracle_ClientID}; code_challenge={Oracle_CodeChallenge}; code_challenge_method=S256; redirect_uri=apiaccount://callback; response_type=code; state="   
    }
    
    response = requests.request("GET", url, headers=headers, data=payload)

    #return response.text
    return response.status_code

# 2. sign-in
def signin():
    url = f"{Oracle_AuthURL}/oidc-provider/v1/oauth2/signin"

    payload = f"username={Oracle_APIUsername}&password={Oracle_APIPassword}&orgname={Oracle_APIOrgName}"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': f"client_id={Oracle_ClientID}; code_challenge={Oracle_CodeChallenge}; code_challenge_method=S256; redirect_uri=apiaccount://callback; response_type=code; state="    
    }      

    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text

authorize()
print(authorize())
# 2a. get auth_code
signin_response = json.loads(signin())
print(signin_response)
redirect_url = signin_response['redirectUrl']
# print(redirect_url)
auth_code = redirect_url.split('code=')[1]

# 3. OAuth Token
def oauth_token():
  url = f"{Oracle_AuthURL}/oidc-provider/v1/oauth2/token"

  payload = f"scope=openid&grant_type=authorization_code&client_id={Oracle_ClientID}&code_verifier={Oracle_CodeVerifier}&code={auth_code}&redirect_uri=apiaccount://callback"
  headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': f"client_id={Oracle_ClientID}; code_challenge={Oracle_CodeChallenge}; code_challenge_method=S256; redirect_uri=apiaccount://callback; response_type=code; state="    
  }

  response = requests.request("POST", url, headers=headers, data=payload)
  return response.text

# 3a. get tokens
oauth_token_response = json.loads(oauth_token())
access_token = oauth_token_response['access_token']

print(access_token)
# 4. set headers
headers = {
    'Content-Type': 'application/json',
    'Authorization': f"Bearer {access_token}",
    'Cookie': f"client_id='{Oracle_ClientID}'; code_challenge='{Oracle_CodeChallenge}'; code_challenge_method=S256; redirect_uri=apiaccount://callback; response_type=code; state="
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# set location

if locRef == 'all':
    # get latest locations from API
    url = f"{Oracle_BaseURL}/bi/v1/{Oracle_APIOrgName}/getLocationDimensions"
    payload = json.dumps({
        "applicationName": "Postman"
    })
    response = requests.request("POST", url, headers=headers, data=payload)

    response_rdd = spark.sparkContext.parallelize([response.content.decode()])
    df_all_locations = spark.read.json(response_rdd)
    df_all_locations = df_all_locations.select(explode("locations")) \
        .select("col.locRef")

    # get exclusion list
    file_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{de_lh_control_id}/Files/seeds/locRef_exclusion_list.txt"
    df_excluded_locations = spark.read.csv(file_path, header=True, inferSchema=True)

    # get list of locations to process
    df_location = df_all_locations.subtract(df_excluded_locations)
    df_location = df_location.orderBy("locRef")
    ls_location = df_location.select("locRef").rdd.flatMap(lambda x: x).collect()

else:
    ls_location = locRef.split(',')

spark = SparkSession.builder.appName("RunParallelJobs").getOrCreate()
location_rdd = spark.sparkContext.parallelize(ls_location)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# stage dimensions

# udf to be applied to each location in RDD
def stage_dimension(locRef):
    # get data
    url = f"{Oracle_BaseURL}/bi/v1/{Oracle_APIOrgName}/{source_entity_name}"
    payload = json.dumps({
        "applicationName": "Postman",
        "locRef": f"{locRef}" 
    })
    response = requests.request("POST", url, headers=headers, data=payload)

    # write to json file
    request_date = datetime.now().strftime("%Y%m%d%H%M%S")
    if response.status_code == 200:
        output_file_path = f"//lakehouse/default/Files/{sink_entity_name}/{request_date}_{locRef}_{sink_entity_name}.json"
    else:
        output_file_path = f"//lakehouse/default/Files/bad_response/{request_date}_{locRef}_{sink_entity_name}.json"

    if not os.path.exists(os.path.dirname(output_file_path)):
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    with open(output_file_path, 'wb') as f:
        f.write(response.content)
    
    return response.status_code

# loop through entities and stage dimensions on each location in the RDD in parallel
# get list of Entities from ETLLoads
df_entity = spark.sql(eval(f"\"select * from DE_LH_CONTROL.ETLLoads where LoadGroup = '{load_group}' and LoadType = 'full' and Disabled = 0\""))
ls_entity = df_entity.collect()

for entity in ls_entity:
    source_entity_name = entity.SourceEntityName
    sink_entity_name = entity.SinkEntityName
    print(sink_entity_name)

    response_rdd = location_rdd.map(stage_dimension)
    results = response_rdd.collect()
    print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# stage OperationsDailyTotals

source_entity_name = 'getOperationsDailyTotals'
sink_entity_name = 'operationsDailyTotals'

# get list of Dates
start_date = datetime.strptime(f'{guestChecks_start_date}', '%Y-%m-%d')
end_date = datetime.strptime(f'{guestChecks_end_date}', '%Y-%m-%d')
ls_date = [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') 
                for x in range((end_date - start_date).days + 1)]
print(ls_date)

# udf to be applied to each location in RDD
def stage_guestChecks(locRef):

    
    # get data
    url = f"{Oracle_BaseURL}/bi/v1/{Oracle_APIOrgName}/{source_entity_name}"
    payload = json.dumps({
        "applicationName": "Postman",
        "locRef": f"{locRef}",
        "busDt": f"{date}"
    })
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response)
    # write to json file
    request_date = datetime.now().strftime("%Y%m%d%H%M%S")
    if response.status_code == 200:
        output_file_path = f"//lakehouse/default/Files/{sink_entity_name}/{request_date}_{locRef}_{date}_{sink_entity_name}.json"
    else:
        output_file_path = f"//lakehouse/default/Files/bad_response/{request_date}_{locRef}_{date}_{sink_entity_name}.json"

    if not os.path.exists(os.path.dirname(output_file_path)):
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    with open(output_file_path, 'wb') as f:
        f.write(response.content)
    
    return response.status_code

# loop through dates and stage guestChecks on each location in the RDD in parallel
for date in ls_date:
    print(date)

    response_rdd = location_rdd.map(stage_guestChecks)
    results = response_rdd.collect()
    #print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(ls_location)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# stage Closed guestChecks

source_entity_name = 'getGuestChecks'
sink_entity_name = 'guestChecks'

# get list of Dates
start_date = datetime.strptime(f'{guestChecks_start_date}', '%Y-%m-%d')
end_date = datetime.strptime(f'{guestChecks_end_date}', '%Y-%m-%d')
ls_date = [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') 
                for x in range((end_date - start_date).days + 1)]
print(ls_date)

# udf to be applied to each location in RDD
def stage_guestChecks(locRef):

    # get data
    url = f"{Oracle_BaseURL}/bi/v1/{Oracle_APIOrgName}/{source_entity_name}"
    payload = json.dumps({
        "applicationName": "Postman",
        "locRef": f"{locRef}",
        "clsdBusDt": f"{date}"
    })
    response = requests.request("POST", url, headers=headers, data=payload)

    # write to json file
    request_date = datetime.now().strftime("%Y%m%d%H%M%S")
    if response.status_code == 200:
        output_file_path = f"//lakehouse/default/Files/{sink_entity_name}/{request_date}_{locRef}_{date}_{sink_entity_name}.json"
    else:
        output_file_path = f"//lakehouse/default/Files/bad_response/{request_date}_{locRef}_{date}_{sink_entity_name}.json"

    if not os.path.exists(os.path.dirname(output_file_path)):
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    with open(output_file_path, 'wb') as f:
        f.write(response.content)
    
    return response.status_code

# loop through dates and stage guestChecks on each location in the RDD in parallel
for date in ls_date:
    print(date)

    response_rdd = location_rdd.map(stage_guestChecks)
    results = response_rdd.collect()
    #print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# stage Updated guestChecks

source_entity_name = 'getGuestChecks'
sink_entity_name = 'guestChecks'
changed_since = f"{guestChecks_off_till_changed_since_datetime}"[:-1]

# get list of Dates
start_date = datetime.strptime(f'{guestChecks_off_till_start_date}', '%Y-%m-%d')
end_date = datetime.strptime(f'{guestChecks_off_till_end_date}', '%Y-%m-%d')
ls_date = [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') 
             for x in range((end_date - start_date).days + 1)]
print(ls_date)

# udf to be applied to each location in RDD
def stage_updated_guestChecks(locRef):

    # get data
    #url = "https://simphony-home.mte4.oraclemicros.com/bi/v1/KIF/getGuestChecks"
    url = f"{Oracle_BaseURL}/bi/v1/{Oracle_APIOrgName}/{source_entity_name}"
    payload = json.dumps({
        "applicationName": "Postman",
        "locRef": f"{locRef}",
        #"clsdGuestChecksOnly": true,
        "clsdBusDt": f"{date}",
        "changedSinceUTC": f"{changed_since}"
    })
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.status_code)

    # write to json file only if there's any data
    request_date = datetime.now().strftime("%Y%m%d%H%M%S")
    print(request_date)
    if('guestChecks": [ ]' not in response.content.decode('utf-8')):
        request_date = datetime.now().strftime("%Y%m%d%H%M%S")
        output_file_path = f"//lakehouse/default/Files/{sink_entity_name}/{request_date}_{locRef}_{date}_{sink_entity_name}_offsite_{guestChecks_off_till_changed_since_datetime}.json"
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        with open(output_file_path, 'wb') as f:
            f.write(response.content)

        return response.status_code
    
    else:
        return ("[ ]")
# loop through dates and stage guestChecks on each location in the RDD in parallel
for date in ls_date:
    print(date)

    response_rdd = location_rdd.map(stage_updated_guestChecks)
    results = response_rdd.collect()
    print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# stage menuItemDailyTotals (prep_cost)

source_entity_name = 'getMenuItemDailyTotals'
sink_entity_name = 'menuItemDailyTotals'

# get list of Dates
start_date = datetime.strptime(f'{menuItemDailyTotals_start_date}', '%Y-%m-%d')
end_date = datetime.strptime(f'{menuItemDailyTotals_end_date}', '%Y-%m-%d')
ls_date = [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') 
                for x in range((end_date - start_date).days + 1)]

print(ls_date)

# udf to be applied to each location in RDD
def stage_menuItemDailyTotals(locRef):

    # get data
    url = f"{Oracle_BaseURL}/bi/v1/{Oracle_APIOrgName}/{source_entity_name}"
    payload = json.dumps({
        "applicationName": "Postman",
        "locRef": f"{locRef}",
        "busDt": f"{date}"
    })
    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.status_code)

    # write to json file
    request_date = datetime.now().strftime("%Y%m%d%H%M%S")
    if response.status_code == 200:
        output_file_path = f"//lakehouse/default/Files/{sink_entity_name}/{request_date}_{locRef}_{date}_{sink_entity_name}.json"
    else:
        output_file_path = f"//lakehouse/default/Files/bad_response/{request_date}_{locRef}_{date}_{sink_entity_name}.json"

    if not os.path.exists(os.path.dirname(output_file_path)):
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    with open(output_file_path, 'wb') as f:
        f.write(response.content)
    
    return response.status_code

# loop through dates and stage menuItemDailyTotals on each location in the RDD in parallel
for date in ls_date:
    print(date)

    response_rdd = location_rdd.map(stage_menuItemDailyTotals)
    results = response_rdd.collect()
    print(results)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# stage ONE menuItemDailyTotals (prep_cost)

source_entity_name = 'getMenuItemDailyTotals'
sink_entity_name = 'menuItemDailyTotals'

date ='2024-09-13'
locRef = 3002

# get data
url = f"{Oracle_BaseURL}/bi/v1/{Oracle_APIOrgName}/{source_entity_name}"
payload = json.dumps({
    "applicationName": "Postman",
    "locRef": f"{locRef}",
    "busDt": f"{date}"
})
response = requests.request("POST", url, headers=headers, data=payload)

print(response.status_code)

# write to json file
request_date = datetime.now().strftime("%Y%m%d%H%M%S")
print(request_date)

if response.status_code == 200:
    output_file_path = f"//lakehouse/default/Files/{sink_entity_name}/{request_date}_{locRef}_{date}_{sink_entity_name}.json"
    print(output_file_path)
else:
    output_file_path = f"//lakehouse/default/Files/bad_response/{request_date}_{locRef}_{date}_{sink_entity_name}.json"

if not os.path.exists(os.path.dirname(output_file_path)):
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

print("sn")

with open(output_file_path, 'wb') as f:
    f.write(response.content)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# stage kdsDetails (prep_time)

source_entity_name = 'getKDSDetails'
sink_entity_name = 'kdsDetails'

# get list of Dates
start_date = datetime.strptime(f'{kdsDetails_start_date}', '%Y-%m-%d')
end_date = datetime.strptime(f'{kdsDetails_end_date}', '%Y-%m-%d')
ls_date = [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') 
                for x in range((end_date - start_date).days + 1)]

print(ls_date)

# udf to be applied to each location in RDD
def stage_kdsDetails(locRef):

    # get data
    url = f"{Oracle_BaseURL}/bi/v1/{Oracle_APIOrgName}/{source_entity_name}"
    payload = json.dumps({
        "applicationName": "Postman",
        "locRef": f"{locRef}",
        "busDt": f"{date}"
    })
    response = requests.request("POST", url, headers=headers, data=payload)

    # write to json file
    request_date = datetime.now().strftime("%Y%m%d%H%M%S")
    if response.status_code == 200:
        output_file_path = f"//lakehouse/default/Files/{sink_entity_name}/{request_date}_{locRef}_{date}_{sink_entity_name}.json"
    else:
        output_file_path = f"//lakehouse/default/Files/bad_response/{request_date}_{locRef}_{date}_{sink_entity_name}.json"

    if not os.path.exists(os.path.dirname(output_file_path)):
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    with open(output_file_path, 'wb') as f:
        f.write(response.content)
    
    return response.status_code

# loop through dates and stage kdsDetails on each location in the RDD in parallel
for date in ls_date:
    print(date)

    response_rdd = location_rdd.map(stage_kdsDetails)
    results = response_rdd.collect()
    #print(results)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
