from src.lambda_functions.ingestion_lambda import get_sales_order_util
import json
from unittest.mock import patch, Mock
from test_data.sales_order_data import rows, columns

user = "test"
host = "test"
db = "test"
password = "test"
port = "test"


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_should_return_a_dict(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = get_sales_order_util(user, host, db, port, password)[0]
    assert type(result) is dict


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_dictionary_should_have_all_required_keys(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    expected = 'keys are present'
    result = get_sales_order_util(user, host, db, port, password)[0]
    for currency in result['data']:
        if all(key in currency for key in ('sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id')):
            are_keys_present = "keys are present"
        else:
            are_keys_present = 'keys are not present'
            break
    assert expected == are_keys_present


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_dictionary_should_be_ordered_by_sales_order_id(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    expected = "Correct Order"
    result = get_sales_order_util(user, host, db, port, password)[0]
    x = result['data'][0]["sales_order_id"]
    for currency in result['data']:
        if currency["sales_order_id"] >= x:
            checker = "Correct Order"
            x = currency["sales_order_id"]
        else:
            checker = 'Incorrect Order'
            break
    assert expected == checker


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_created_at_and_last_updated_should_be_strings(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    expected = 'yes'
    function = get_sales_order_util(user, host, db, port, password)[0]
    for value in function['data']:
        if type(value['created_at']) == str and type(value['last_updated']) == str:
            are_dates_strings = 'yes'
        else:
            are_dates_strings = 'no'
            break
    assert expected == are_dates_strings


@patch("src.lambda_functions.ingestion_lambda.pg8000.native.Connection")
def test_can_be_converted_to_json(mock_connection):
    mock_connection().run = Mock(return_value=rows)
    mock_connection().columns = columns
    result = json.dumps(get_sales_order_util(
        user, host, db, port, password)[0])
    assert type(result) == str
