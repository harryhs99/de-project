from src.lambda_functions.ingestion_lambda import get_transaction_data
import json
from unittest.mock import patch, Mock
from test_data.transaction_data import rows, columns


user = "test"
host = "test"
db = "test"
password = "test"
port = "test"

# tests for get_counter_party data


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_should_return_a_dict(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_transaction_data(user, host, db, port, password)[0]
    assert type(result) is dict


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_should_have_a_key_of_data(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_transaction_data(user, host, db, port, password)[0]
    assert 'data' in result


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_data_key_should_contain_list_of_valid_dict_rows(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_transaction_data(user, host, db, port, password)[0]['data']
    for row in result:
        assert 'transaction_id' in row
        assert 'transaction_type' in row
        assert 'sales_order_id' in row
        assert 'purchase_order_id' in row
        assert 'created_at' in row
        assert 'last_updated' in row


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_data_for_correct_type(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_transaction_data(user, host, db, port, password)[0]['data']
    for row in result:
        assert type(row['transaction_id']) is int
        assert type(row['transaction_type']) is str
        assert type(row['sales_order_id']) is int or type(None)
        assert type(row['purchase_order_id']) is int or type(None)
        assert type(row['created_at']) is str
        assert type(row['last_updated']) is str


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_can_be_converted_to_json(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = json.dumps(get_transaction_data(
        user, host, db, port, password)[0])
    assert type(result) == str
