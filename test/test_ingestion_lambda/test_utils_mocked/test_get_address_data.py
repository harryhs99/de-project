from src.lambda_functions.ingestion_lambda import get_address_data
import json
from unittest.mock import patch, Mock
from test_data.address_data import rows, columns

user = "test"
host = "test"
db = "test"
password = "test"
port = "test"


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_should_return_a_dict(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_address_data(user, host, db, port, password)[0]
    assert type(result) is dict


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_dictionary_should_have_all_required_keys(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    expected = "keys are present"
    result = get_address_data(user, host, db, port, password)[0]
    for address in result["data"]:
        if all(
            key in address
            for key in (
                "address_id",
                "address_line_1",
                "address_line_2",
                "district",
                "city",
                "postal_code",
                "country",
                "phone",
                "created_at",
                "last_updated",
            )
        ):
            are_keys_present = "keys are present"
        else:
            are_keys_present = "keys are not present"
            break
    assert expected == are_keys_present


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_dictionary_should_be_ordered_by_address_id(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    expected = "Correct Order"
    result = get_address_data(user, host, db, port, password)[0]
    x = result["data"][0]["address_id"]
    for address in result["data"]:
        if address["address_id"] >= x:
            checker = "Correct Order"
            x = address["address_id"]
        else:
            checker = "Incorrect Order"
            break
    assert expected == checker


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_created_at_and_last_updated_should_be_strings(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    expected = "yes"
    function = get_address_data(user, host, db, port, password)[0]
    for value in function["data"]:
        print(type(value["created_at"]))
        if type(value["created_at"]) == str and type(value["last_updated"]) == str:
            are_dates_strings = "yes"
        else:
            are_dates_strings = "no"
            break
    assert expected == are_dates_strings


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_can_be_converted_to_json(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = json.dumps(get_address_data(user, host, db, port, password)[0])
    assert type(result) == str
