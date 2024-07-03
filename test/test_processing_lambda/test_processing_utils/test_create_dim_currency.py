from unittest.mock import patch
import json
from src.lambda_functions.processed_lambda import create_dim_currency
import pytest


test_data = {
    "data": [
        {
            "currency_id": 1,
            "currency_code": "GBP",
            "created_at": "2022-11-03, 14:20:49:962000",
            "last_updated": "2022-11-03, 14:20:49:962000",
        },
        {
            "currency_id": 2,
            "currency_code": "USD",
            "created_at": "2022-11-03, 14:20:49:962000",
            "last_updated": "2022-11-03, 14:20:49:962000",
        },
        {
            "currency_id": 3,
            "currency_code": "EUR",
            "created_at": "2022-11-03, 14:20:49:962000",
            "last_updated": "2022-11-03, 14:20:49:962000",
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


def test_should_have_(extraction_patched, latest_file_name_patched):
    extraction_patched.return_value = json.dumps(test_data)
    latest_file_name_patched.return_value = None
    expected_columns = ["currency_id", "currency_code", "currency_name"]
    assert list(create_dim_currency()[0].columns) == expected_columns


def test_should_have_correct_data_types(extraction_patched, latest_file_name_patched):
    extraction_patched.return_value = json.dumps(test_data)
    latest_file_name_patched.return_value = None
    expected_data_types = {
        "currency_code": "object", "currency_name": "object"}
    data_frame = create_dim_currency()[0]
    for column, data_type in expected_data_types.items():
        assert data_frame[column].dtype == data_type
