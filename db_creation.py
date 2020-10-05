import boto3

s3 = boto3.resource('s3', aws_access_key_id='AKIAXSN47F25TZLWXVMS', aws_secret_access_key='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')

try:
    s3.create_bucket(Bucket='hjo6-datacont', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
except:
    print("This may already exist")

bucket = s3.Bucket("hjo6-datacont")

bucket.Acl().put(ACL='public-read')

body = open('/Users/hunterosterhoudt/desktop/school/cs1660/homework-2/hello-world.txt', 'rb')

o = s3.Object('hjo6-datacont', 'test').put(Body=body)
s3.Object('hjo6-datacont', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb', region_name='us-west-2', aws_access_key_id='AKIAXSN47F25TZLWXVMS', aws_secret_access_key='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')

try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    table = dyndb.Table("DataTable")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)

import csv
urlbase = "https://s3-us-west-2.amazonaws.com/hjo6-datacont/"
with open('/Users/hunterosterhoudt/desktop/school/cs1660/homework-2/experiments.csv', 'rt') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print(item)
        body = open('/Users/hunterosterhoudt/desktop/school/cs1660/homework-2/datafiles/'+item[3], 'rb')
        s3.Object('hjo6-datacont', item[3]).put(Body=body)
        md = s3.Object('hjo6-datacont', item[3]).Acl().put(ACL='public-read')
        url = urlbase + item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1], 'description': item[4], 'date': item[2], 'url': url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("Item already exists or failure")

response = table.get_item(
    Key={
        'PartitionKey': 'experiment3',
        'RowKey': 'data3'
    }
)
item = response['Item']
print(item)

response
