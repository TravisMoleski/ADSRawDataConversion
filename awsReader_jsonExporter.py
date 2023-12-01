import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import logging
from matplotlib import pyplot as plt
import time
import json
from decimal import Decimal
import os

##### CODE INIT #####

# random vars
scriptStart = time.time()
last_evaluated_key = None
responses = []

# Dynamodb setup
# Create a DynamoDB local client
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
 
# Define the table name
table_name = 'coll_test_3'
 
# Get the DynamoDB table
table = dynamodb.Table(table_name)

# json setup
json_idx = 0
json_export_folder = table_name + '_' + str(time.time())
if not os.path.exists(json_export_folder):
    os.makedirs(json_export_folder)

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)  # Convert Decimal to string
        return super(DecimalEncoder, self).default(o)  
     
 
##### MAIN LOOP #####

pose_response = table.query(
    IndexName='TopicIndex',  # Specify the name of the global secondary index
    KeyConditionExpression=Key('topic').eq('/apollo/localization/pose')
)

while True:
    
    if last_evaluated_key:
        pose_response = table.query(
            IndexName='TopicIndex',
            KeyConditionExpression=Key('topic').eq('/apollo/localization/pose'),
            ExclusiveStartKey=last_evaluated_key
        )
    else:
        pose_response = table.query(
            IndexName='TopicIndex',
            KeyConditionExpression=Key('topic').eq('/apollo/localization/pose')
        )
        
    # json_data = json.dumps(response, indent=2)
    
    json_file_name = os.path.join(json_export_folder, str(table_name) + '_' + str(json_idx) + '.json')
                 
    with open(json_file_name, 'w') as json_file:
        json.dump(pose_response, json_file, indent=2, cls=DecimalEncoder)
    json_idx += 1
    
    # responses.extend(response.get('Items', []))
    last_evaluated_key = pose_response.get('LastEvaluatedKey')

    if not last_evaluated_key:
        break
    
    print(f"Found... {len(pose_response)} entries for table: {table} in key {last_evaluated_key}")


queryTime = time.time() - scriptStart

print("QUERY TIME:", queryTime)

# print(type(response))
# print(len(response))
 
# xList  = []
# yList  = []
# zList  = []
# items = response.get('Items', [])
# for item in items:
#     xList.append(item['pose']['position']['x'])
#     yList.append(item['pose']['position']['y'])
 
# print("Total time, ", time.time()-scriptStart)
 
# print("FOUND: ", len(items), "ENTRIES")
# plt.xlabel("X UTM (m)")
# plt.ylabel("Y UTM (m)")
# plt.axis('equal')
# plt.scatter(xList,yList)
# plt.grid(True)
# plt.show()