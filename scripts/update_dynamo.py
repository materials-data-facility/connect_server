import json
import re
from decimal import Decimal
from time import sleep

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

RETRY_EXCEPTIONS = ('ProvisionedThroughputExceededException',
                    'ThrottlingException')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('prod-status-alpha-1')
dest_table = dynamodb.Table('dev-status-0.4')
print(table)
scan_kwargs = {}

done = False
start_key = None
i = 0
version_re = re.compile("(.+)_(v[0-9].*$)")
while not done:
    if i > 10:
        break
    i = i + 1
    if start_key:
        scan_kwargs['ExclusiveStartKey'] = start_key
    response = table.scan(**scan_kwargs)
    items = response.get('Items', [])
    for item in items:
        match = version_re.match(item['source_id'])
        if not match:
            print("--------->", item['source_id'])
        else:
            source_name = match.group(1)
            version = match.group(2).replace("-", ".")
            if "." not in version:
                version = version + ".0"

            # Remove the leading v
            version = version[1:]
            print(item['source_id'], f"[{version}] ({source_name})")
            new_rec = item.copy()
            new_rec['version'] = version
            original_submission = json.loads(item['original_submission'])
            new_rec['source_id'] = original_submission.get('source_name', source_name)

            retries = 0
            success = False
            while not success:
                try:
                    dest_table.put_item(Item=new_rec)
                    retries = 0
                    success = True
                except ClientError as err:
                    if err.response['Error']['Code'] not in RETRY_EXCEPTIONS:
                        raise
                    print('WHOA, too fast, slow it down retries={}'.format(retries))
                    sleep(2 ** retries)
                    retries += 1  # TODO max limit

    start_key = response.get('LastEvaluatedKey', None)
    done = start_key is None


