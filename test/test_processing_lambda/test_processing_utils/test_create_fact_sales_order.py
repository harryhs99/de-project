from unittest.mock import patch
import json
from src.lambda_functions.processed_lambda import create_fact_sales_order
import pytest

test_sales_order_data = {
    "data": [
        {
            "sales_order_id": 1,
            "created_at": "2022-11-03, 14:20:52:186000",
            "last_updated": "2022-11-03, 14:20:52:186000",
            "design_id": 9,
            "staff_id": 16,
            "counterparty_id": 18,
            "units_sold": 84754,
            "unit_price": "2.43",
            "currency_id": 3,
            "agreed_delivery_date": "2022-11-10",
            "agreed_payment_date": "2022-11-03",
            "agreed_delivery_location_id": 4
        },
        {
            "sales_order_id": 2,
            "created_at": "2022-11-03, 14:20:52:186000",
            "last_updated": "2022-11-03, 14:20:52:186000",
            "design_id": 3,
            "staff_id": 19,
            "counterparty_id": 8,
            "units_sold": 42972,
            "unit_price": "3.94",
            "currency_id": 2,
            "agreed_delivery_date": "2022-11-07",
            "agreed_payment_date": "2022-11-08",
            "agreed_delivery_location_id": 8
        },
        {
            "sales_order_id": 3,
            "created_at": "2022-11-03, 14:20:52:188000",
            "last_updated": "2022-11-03, 14:20:52:188000",
            "design_id": 4,
            "staff_id": 10,
            "counterparty_id": 4,
            "units_sold": 65839,
            "unit_price": "2.91",
            "currency_id": 3,
            "agreed_delivery_date": "2022-11-06",
            "agreed_payment_date": "2022-11-07",
            "agreed_delivery_location_id": 19
        },
    ]
}


@pytest.fixture
def extraction_patched():
    with patch("src.lambda_functions.processed_lambda.extract_data_from_ingestion_s3") as mock:
        yield mock


@pytest.fixture
def latest_file_name_patched():
    with patch("src.lambda_functions.processed_lambda.get_latest_file_name") as mock:
        yield mock


def test_should_have_correct_columns(extraction_patched, latest_file_name_patched):
    extraction_patched.return_value = json.dumps(test_sales_order_data)
    latest_file_name_patched.return_value = None
    expected_columns = ['sales_order_id',
                        'created_date',
                        'created_time',
                        'last_updated_date',
                        'last_updated_time',
                        'staff_id',
                        'counterparty_id',
                        'units_sold',
                        'unit_price',
                        'currency_id',
                        'design_id',
                        'agreed_payment_date',
                        'agreed_delivery_date',
                        'agreed_delivery_location_id']
    assert list(create_fact_sales_order()[0].columns) == expected_columns


def test_should_have_correct_data_types(extraction_patched, latest_file_name_patched):
    extraction_patched.return_value = json.dumps(test_sales_order_data)
    latest_file_name_patched.return_value = None
    expected_data_types = {
        'sales_order_id': 'int64',
        'created_date': 'datetime64[ns]',
        'created_time': 'object',
        'last_updated_date': 'datetime64[ns]',
        'last_updated_time': 'object',
        'staff_id': 'int64',
        'counterparty_id': 'int64',
        'units_sold': 'int64',
        'unit_price': 'float64',
        'currency_id': 'int64',
        'design_id': 'int64',
        'agreed_payment_date': 'datetime64[ns]',
        'agreed_delivery_date': 'datetime64[ns]',
        'agreed_delivery_location_id': 'int64'
    }
    data_frame = create_fact_sales_order()[0]
    for column, data_type in expected_data_types.items():
        assert data_frame[column].dtype == data_type
