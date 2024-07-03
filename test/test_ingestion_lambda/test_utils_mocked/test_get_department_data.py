from src.lambda_functions.ingestion_lambda import get_department_data
import json
from test_data.department_data import rows, columns
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
    result = get_department_data(user, host, db, port, password)[0]
    assert type(result) is dict


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_should_have_a_key_of_data(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_department_data(user, host, db, port, password)[0]
    assert "data" in result


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_data_key_should_contain_list_of_valid_dict_rows(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_department_data(user, host, db, port, password)[0]
    for row in result["data"]:
        assert "department_id" in row
        assert "department_name" in row
        assert "location" in row
        assert "manager" in row
        assert "created_at" in row
        assert "last_updated" in row


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_data_for_correct_type(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_department_data(user, host, db, port, password)[0]
    for row in result["data"]:
        print(row)
        print(type(row["department_id"]))
        assert type(row["department_id"]) is int
        assert type(row["department_name"]) is str
        assert type(row["location"]) is str
        assert type(row["manager"]) is str
        assert type(row["created_at"]) is str
        assert type(row["last_updated"]) is str


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_can_be_converted_to_json(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = json.dumps(get_department_data(user, host, db, port, password)[0])
    assert type(result) == str
