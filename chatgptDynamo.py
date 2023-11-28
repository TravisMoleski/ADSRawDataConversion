import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import logging
from matplotlib import pyplot as plt

import time

def extractAll(table):
    responses = []
    while True:
        print("Found...", len(responses), " entries for table:", table)
        if last_evaluated_key:
            response = table.scan(
                TableName= table,
                ExclusiveStartKey=last_evaluated_key
            )
        else: 
            response = table.scan(TableName= table)
        last_evaluated_key = response.get('LastEvaluatedKey')
        responses.extend(response)
    
        if not last_evaluated_key:
            break
    return responses

scriptStart = time.time()

# Create a DynamoDB local client
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:5000')

# Define the table name
table_name = 'cyber_aws'
meta_name  = 'cyber_aws_meta'
experimentID = 34

# Get the DynamoDB table
cybertable = dynamodb.Table(table_name)
metatable  = dynamodb.Table(meta_name)

print(metatable.creation_date_time)

last_evaluated_key = None

print("GET META...")
meta_responses = []
while True:
    print("SCANNED: ", meta_name, len(meta_responses), "times")
    if last_evaluated_key:
        response = metatable.scan(
            TableName= meta_name,
            ExclusiveStartKey=last_evaluated_key,
            FilterExpression=Key("experimentID").eq(experimentID)
        )
    else: 
        response = metatable.scan(TableName= meta_name, FilterExpression=Key("experimentID").eq(experimentID))
    last_evaluated_key = response.get('LastEvaluatedKey')
    meta_responses.append(response)

    if not last_evaluated_key:
        break
# meta_responses = metatable.scan(FilterExpression=Attr('experimentID').eq(34))
metaFileCount = 0
for mResponse in meta_responses:
    metaFileCount += mResponse['Count']
print("FOUND: ", metaFileCount, "FILES", "WITH EXPERIMENT ID", experimentID)

queryTime = time.time() - scriptStart
print("META QUERY TIME:", queryTime)

meta_item = meta_response['Items']

for item in meta_item:
    print(item['_id'])

metadataID = meta_item[2]['_id']
print("LOOKING FOR META ID: ", metadataID)

queryTime = time.time() - scriptStart
print("META QUERY TIME:", queryTime)

cyber_data_response = cybertable.scan(
    FilterExpression=Attr('topic').eq('/apollo/localization/pose')
)
cyber_items = cyber_data_response['Items']
queryTime = time.time() - scriptStart
print("CYBER QUERY TIME:", queryTime)

xList  = []
yList  = []
zList  = []
for item in cyber_items:
    xList.append(item['pose']['position']['x'])
    yList.append(item['pose']['position']['y'])

print("Total time, ", time.time()-scriptStart)

print("FOUND: ", len(cyber_items), "ENTRIES")
plt.xlabel("X UTM (m)")
plt.ylabel("Y UTM (m)")
plt.axis('equal')
plt.scatter(xList,yList)
plt.grid(True)
plt.show()
