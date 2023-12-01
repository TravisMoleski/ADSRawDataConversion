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

# Dynamodb setup
# Create a DynamoDB local client
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
 
# Define the table name
table_name = 'coll_test_5'
 
# Get the DynamoDB table
table = dynamodb.Table(table_name)

# json setup
json_export_folder = table_name + '_' + str(round(time.time()))
if not os.path.exists(json_export_folder):
    os.makedirs(json_export_folder)
    print(f"Output folder: {json_export_folder}")
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)  # Convert Decimal to string
        return super(DecimalEncoder, self).default(o)  

class DynamoDB_Query:
    
    def __init__(self, topic, table_name, table, json_export_folder):
        
        # VAR INIT
        last_evaluated_key = None
        json_idx = 0
        queryStartTime = time.time()
        topic_export_string = topic.replace('/','_')
        
        pose_response = table.query(
            IndexName='TopicIndex',  # Specify the name of the global secondary index
            KeyConditionExpression=Key('topic').eq(topic)
        )

        # MAIN LOOP
        while True:
            
            if last_evaluated_key:
                pose_response = table.query(
                    IndexName='TopicIndex',
                    KeyConditionExpression=Key('topic').eq(topic),
                    ExclusiveStartKey=last_evaluated_key
                )
            else:
                pose_response = table.query(
                    IndexName='TopicIndex',
                    KeyConditionExpression=Key('topic').eq(topic)
                )
            
            json_file_name =  str(table_name) + str(topic_export_string) + '_' + str(json_idx) + '.json'
            json_file_name = os.path.join(json_export_folder, json_file_name)
                        
            with open(json_file_name, 'w') as json_file:
                json.dump(pose_response, json_file, indent=2, cls=DecimalEncoder)
            json_idx += 1
            
            last_evaluated_key = pose_response.get('LastEvaluatedKey')

            if not last_evaluated_key:
                break
            
            print(f"Found... {len(pose_response)} entries for table: {table} in key {last_evaluated_key}")


        queryEndTime = time.time() - queryStartTime

        print("QUERY TIME:", queryEndTime)
        


if __name__ == '__main__':

    topics = ['/apollo/localization/pose',
              '/apollo/perception/traffic_light',
              '/apollo/sensor/gnss/best_pose',
              '/apollo/canbus/chassis']
    
    for topic in topics:
        print(topic)
        DynamoDB_Query(topic, table_name, table, json_export_folder)