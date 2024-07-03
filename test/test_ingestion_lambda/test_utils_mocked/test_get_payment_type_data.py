from src.lambda_functions.ingestion_lambda import get_payment_type_data
import json
from unittest.mock import patch, Mock
from test_data.payment_type_data import rows, columns

user = "test"
host = "test"
db = "test"
password = "test"
port = "test"

# tests for get_payment_type data


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_should_return_a_dict(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_payment_type_data(user, host, db, port, password)[0]
    assert type(result) is dict


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_should_have_a_key_of_data(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_payment_type_data(user, host, db, port, password)[0]
    assert 'data' in result


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_data_key_should_contain_list_of_valid_dict_rows(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_payment_type_data(user, host, db, port, password)[0]['data']
    for row in result:
        assert 'payment_type_id' in row
        assert 'payment_type_name' in row
        assert 'created_at' in row
        assert 'last_updated' in row


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_data_should_look_correct(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_payment_type_data(user, host, db, port, password)[0]['data']
    for row in result:
        assert type(row['payment_type_id']) is int
        assert type(row['payment_type_name']) is str
        assert type(row['created_at']) is str
        assert type(row['last_updated']) is str


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_can_be_converted_to_json(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = json.dumps(get_payment_type_data(
        user, host, db, port, password)[0])
    assert type(result) == str
