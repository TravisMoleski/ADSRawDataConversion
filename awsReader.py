import sys
import json
from datetime import datetime
import argparse
from decimal import Decimal
from databaseinterface import DatabaseInterface
import logging
from os import environ as env

import boto3
from matplotlib import pyplot as plt


def checkKey(dict, key):
    if(key in dict):
        return True
    return False
    
def main(uri):
    ddb = boto3.resource('dynamodb', endpoint_url=uri,#dynamodb.us-east-2.amazonaws.
                             aws_access_key_id= '123',
                             aws_secret_access_key='123',
                             region_name="us-east-2", )
    tables = []
    print("FINDING TABLES...")
    for table in ddb.tables.all():
        tables.append(table)
    tables[0].load()
    cyber_table = tables[0]
    print("FOUND", tables)

    response = cyber_table.scan()
    data = []
    last_evaluated_key = None

    print("SIFTING...")
    while True:
        print("Found...", len(data), " entries")
        if last_evaluated_key:
            response = cyber_table.scan(
                TableName= 'cyber_aws',
                ExclusiveStartKey=last_evaluated_key
            )
        else: 
            response = cyber_table.scan(TableName= tables[0].name)
        last_evaluated_key = response.get('LastEvaluatedKey')
        data.extend(response['Items'])
        if not last_evaluated_key:
            break

    xList  =[]
    yList  = []
    zList  = []
    colors = []
    color = 'green'
    print("PROCESSING...")
    for item in data:
        print("Found...", len(xList), " locations")
        try:
            topic = item['topic']
            if topic == '/apollo/localization/pose':
                xList.append(item['pose']['position']['x'])
                yList.append(item['pose']['position']['y'])
                colors.append(color)
            # elif topic == '/apollo/canbus/chassis':
            #     d_mode = item['drivingMode']
            #     print(d_mode)
            #     if d_mode == "COMPLETE_AUTO_DRIVE":
            #         color = 'red'
            #     else:
            #         color = 'blue'
        except:
            break
    
    plt.xlabel("X UTM (m)")
    plt.ylabel("Y UTM (m)")
    plt.axis('equal')
    plt.scatter(xList,yList,c=colors)
    plt.grid(True)
    plt.show()

if __name__ == '__main__':
    uri = "http://localhost:5000"
    main(uri)