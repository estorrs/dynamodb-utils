import datetime
import decimal
import json

import boto3

def get_update_expression(payload):
    """get dynamo db update expression for the payload"""
    expressions = []
    attribute_values = {}
    for i, k in enumerate(payload.keys()):
        s = f'{k}=:{i}'
        expressions.append(s)

        attribute_values[f':{i}'] = payload[k]

    expression = 'set ' + ', '.join(expressions)

    return expression, attribute_values

def parse_iso_datetime_str(datetime_str):
    """return datetime parsed from iso formatted string"""
    return datetime.datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%f")


class DynamodbEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            if obj % 1 > 0:
                return float(obj)
            else:
                return int(obj)

        return json.JSONEncoder.default(self, obj)


class DynamodbHelper(object):
    """Base helper class"""
    def __init__(self, aws_access_key, aws_secret_key, aws_region, table_name, table_key):
        self.session = boto3.session.Session(
                aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,
                region_name=aws_region)
        self.resource = self.session.resource('dynamodb')
#         self.client = self.session.client('dynamodb')

        self.table = self.resource.Table(table_name)
        self.table_key = table_key
        self.table_name = table_name

    def get_item(self, key):
        """get item from table with given key"""
        if key is None:
            return None

        response = self.table.get_item(
                Key={
                    self.table_key: key
                }
            )

        item = response.get('Item', None)

        if item is None:
            return None

        encoded_item = json.dumps(item, cls=DynamodbEncoder)

        return json.loads(encoded_item)

    def get_multiple_items(self, keys, attributes_to_get=None):
        """get multiple items from dynamodb table.

        attributes_to_get
            - list of attributes to return. If None returns all attributes
        """
        request_items = {
                self.table_name: {
                    'Keys': [{self.table_key: k} for k in keys],
                    'ConsistentRead': True,
                    }
                }

        if attributes_to_get:
            request_items[self.table_name]['ProjectionExpression'] = ', '.join(
                    attributes_to_get
                    )

        response = self.resource.meta.client.batch_get_item(
                RequestItems=request_items,
                ReturnConsumedCapacity='TOTAL'
                )

        items = response['Responses']

        encoded_items = json.dumps(items, cls=DynamodbEncoder)

        return json.loads(encoded_items)[self.table_name]


    def put_item(self, payload):
        '''add item to dynamo db table'''
        payload = json.dumps(payload)
        payload = json.loads(payload, parse_float=decimal.Decimal)

        self.table.put_item(
                Item=payload
                )

    def update_item(self, key, payload):
        """update item with the given key with items in payload"""
        payload = json.dumps(payload)
        payload = json.loads(payload, parse_float=decimal.Decimal)

        update_expression, attribute_values = get_update_expression(payload)

        self.table.update_item(
                Key={
                    self.table_key: key
                },
                UpdateExpression=update_expression,
                ExpressionAttributeValues=attribute_values,
                ReturnValues="UPDATED_NEW"
                )

    def update_list(self, key, field, values):
        """append value to list in the given field"""
        self.table.update_item(
                Key={
                    self.table_key: key
                },
                UpdateExpression=f'set {field} = list_append({field}, :i)',
                ExpressionAttributeValues={
                        ':i': values,
                        },
                ReturnValues="UPDATED_NEW"
                )
