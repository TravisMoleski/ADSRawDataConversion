import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import logging
from matplotlib import pyplot as plt
import time


scriptStart = time.time()
 
# Create a DynamoDB local client
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
 
# Define the table name
table_name = 'coll_test_3'
 
# Get the DynamoDB table
table = dynamodb.Table(table_name)

print(type(table))
 
 
response = table.query(
    IndexName='TopicIndex',  # Specify the name of the global secondary index
    KeyConditionExpression=Key('topic').eq('/apollo/localization/pose')
)


last_evaluated_key = None
responses = []

while True:
    print("Found...", len(responses), " entries for table:", table)
    
    if last_evaluated_key:
        response = table.query(
            IndexName='TopicIndex',
            KeyConditionExpression=Key('topic').eq('/apollo/localization/pose'),
            ExclusiveStartKey=last_evaluated_key
        )
    else:
        response = table.query(
            IndexName='TopicIndex',
            KeyConditionExpression=Key('topic').eq('/apollo/localization/pose')
        )
    
    responses.extend(response.get('Items', []))
    last_evaluated_key = response.get('LastEvaluatedKey')

    if not last_evaluated_key:
        break
    
queryTime = time.time() - scriptStart

print("QUERY TIME:", queryTime)

# print(type(response))
# print(len(response))
 
xList  = []
yList  = []
zList  = []
items = response.get('Items', [])
for item in items:
    xList.append(item['pose']['position']['x'])
    yList.append(item['pose']['position']['y'])
 
print("Total time, ", time.time()-scriptStart)
 
print("FOUND: ", len(items), "ENTRIES")
plt.xlabel("X UTM (m)")
plt.ylabel("Y UTM (m)")
plt.axis('equal')
plt.scatter(xList,yList)
plt.grid(True)
plt.show()