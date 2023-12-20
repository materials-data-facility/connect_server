import json
import re
import sys
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from aws.dynamo_manager import DynamoManager

dynamo_manager = DynamoManager()
v = dynamo_manager.get_current_version('_test_v1')
print(v)
sys.exit(0)

response = dest_table.query(
        KeyConditionExpression=
            Key('source_name').eq("_test_v1"),
            ScanIndexForward=False
    )

versions = {str(x['version']): x for x in response['Items']}
print(sorted(versions.keys(), key=lambda x:[int(i) if i.isdigit() else i for i in x.split('.')]))


