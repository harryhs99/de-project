from src.lambda_functions.ingestion_lambda import get_counterparty_data
import json
from test_data.counterparty_data import rows, columns
from unittest.mock import patch, Mock

user = "test"
host = "test"
db = "test"
password = "test"
port = "test"


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_should_return_a_dict(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_counterparty_data(user, host, db, port, password)[0]
    assert type(result) is dict


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_should_have_a_key_of_data(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_counterparty_data(user, host, db, port, password)[0]
    assert "data" in result


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_data_key_should_contain_list_of_valid_dict_rows(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_counterparty_data(user, host, db, port, password)[0]["data"]
    for row in result:
        assert "counterparty_id" in row
        assert "counterparty_legal_name" in row
        assert "legal_address_id" in row
        assert "commercial_contact" in row
        assert "delivery_contact" in row
        assert "created_at" in row
        assert "last_updated" in row


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_data_for_correct_type(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_counterparty_data(user, host, db, port, password)[0]["data"]
    for row in result:
        assert type(row["counterparty_id"]) is int
        assert type(row["counterparty_legal_name"]) is str
        assert type(row["legal_address_id"]) is int
        assert type(row["commercial_contact"]) is str
        assert type(row["delivery_contact"]) is str
        assert type(row["created_at"]) is str
        assert type(row["last_updated"]) is str


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_can_be_converted_to_json(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = json.dumps(get_counterparty_data(
        user, host, db, port, password)[0])
    assert type(result) == str
