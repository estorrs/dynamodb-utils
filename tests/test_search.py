import os

import pytest

from dynamodb_utils.helper import DynamodbHelper

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_REGION = os.getenv('AWS_REGION')

TABLE_NAME = os.getenv('TABLE_NAME')
KEY_NAME = os.getenv('KEY_NAME')

helper = DynamodbHelper(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, TABLE_NAME, KEY_NAME)

key = '12345'

def test_put_item():
    payload = {
            KEY_NAME: key,
            'number_x': 2.1,
            'nested': {
                'phrase': [
                    'hello'
                    ]
                }
            }

    helper.put_item(payload)

    result = helper.get_item(key)

    assert payload == result

def test_update_item():
    payload = {
            'number_x': 3.4,
            'nested': {
                'phrase': [
                    'world'
                    ]
                }
            }

    helper.update_item(key, payload)

    result = helper.get_item(key)

    assert result['number_x'] == 3.4
    assert result['nested']['phrase'] == ['world']

def test_update_list():
    field = 'nested.phrase'
    values = ['hello', 'world']

    helper.update_list(key, field, values)

    result = helper.get_item(key)

    assert result['nested']['phrase'] == ['world', 'hello', 'world']
