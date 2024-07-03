import json
from unittest.mock import patch
from src.lambda_functions.processed_lambda import create_dim_location
import pytest


with open('test/test_processing_lambda/test_processing_utils/location_test_data.json') as f:
    location_test_data = json.load(f)


@pytest.fixture
def extraction_patched():
    with patch("src.lambda_functions.processed_lambda.extract_data_from_ingestion_s3") as mock:
        yield mock


@pytest.fixture
def latest_file_name_patched():
    with patch("src.lambda_functions.processed_lambda.get_latest_file_name") as mock:
        yield mock


def test_should_have_correct_columns(extraction_patched, latest_file_name_patched):
    extraction_patched.return_value = json.dumps(location_test_data)
    latest_file_name_patched.return_value = None
    expected_columns = ['location_id', 'address_line_1', 'address_line_2',
                        'district', 'city', 'postal_code', 'country', 'phone']
    assert list(create_dim_location()[0].columns) == expected_columns


def test_should_have_correct_data_types(extraction_patched, latest_file_name_patched):
    extraction_patched.return_value = json.dumps(location_test_data)
    latest_file_name_patched.return_value = None
    expected_data_types = {
        'address_line_1': 'object',
        'address_line_2': 'object',
        'district': 'object',
        'city': 'object',
        'postal_code': 'object',
        'country': 'object',
        'phone': 'object'
    }
    data_frame = create_dim_location()[0]
    for column, data_type in expected_data_types.items():
        assert data_frame[column].dtype == data_type
