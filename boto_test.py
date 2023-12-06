import boto3

TABLE_NAME = "example"

dynamodb = boto3.client(
    "dynamodb",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    endpoint_url="http://localhost:8000",
    region_name="us-east-2",
)

def create_table():
    dynamodb.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    )
    print("Table succesfully created")


def delete_table():
    dynamodb.delete_table(TableName=TABLE_NAME)
    print("Table has been deleted")


def list_tables():
    tables = dynamodb.list_tables()
    print(tables)

list_tables()
create_table()
list_tables()
delete_table()
list_tables()