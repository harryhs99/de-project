import json
from unittest.mock import patch
from src.lambda_functions.processed_lambda import create_dim_transaction
import pytest

with open('test/test_processing_lambda/test_processing_utils/transaction_test_data.json') as f:
    transaction_data = json.load(f)


@pytest.fixture
def extraction_patched():
    with patch("src.lambda_functions.processed_lambda.extract_data_from_ingestion_s3") as mock:
        yield mock


@pytest.fixture
def latest_file_name_patched():
    with patch("src.lambda_functions.processed_lambda.get_latest_file_name") as mock:
        yield mock


def test_should_have_correct_columns(extraction_patched, latest_file_name_patched):
    extraction_patched.return_value = json.dumps(transaction_data)
    latest_file_name_patched.return_value = None
    expected_columns = ['transaction_id',
                        'transaction_type',
                        'sales_order_id',
                        'purchase_order_id']
    assert list(create_dim_transaction()[0].columns) == expected_columns


def test_should_have_correct_data_types(extraction_patched, latest_file_name_patched):
    extraction_patched.return_value = json.dumps(transaction_data)
    latest_file_name_patched.return_value = None
    expected_data_types = {
        'transaction_id': 'int64',
        'transaction_type': 'object',
        'sales_order_id': 'int64',
        'purchase_order_id': 'int64'
    }
    data_frame = create_dim_transaction()[0]
    for column, data_type in expected_data_types.items():
        assert data_frame[column].dtype == data_type
