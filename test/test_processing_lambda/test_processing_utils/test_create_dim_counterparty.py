import json
from unittest.mock import patch
from src.lambda_functions.processed_lambda import create_dim_counterparty
import pytest

counterparty_test_data = {
    "data": [
        {
            "counterparty_id": 1,
            "counterparty_legal_name": "Fahey and Sons",
            "legal_address_id": 15,
            "commercial_contact": "Micheal Toy",
            "delivery_contact": "Mrs. Lucy Runolfsdottir",
            "created_at": "2022-11-03, 14:20:51:563000",
            "last_updated": "2022-11-03, 14:20:51:563000"
        },
        {
            "counterparty_id": 2,
            "counterparty_legal_name": "Leannon, Predovic and Morar",
            "legal_address_id": 28,
            "commercial_contact": "Melba Sanford",
            "delivery_contact": "Jean Hane III",
            "created_at": "2022-11-03, 14:20:51:563000",
            "last_updated": "2022-11-03, 14:20:51:563000"
        },
        {
            "counterparty_id": 3,
            "counterparty_legal_name": "Armstrong Inc",
            "legal_address_id": 2,
            "commercial_contact": "Jane Wiza",
            "delivery_contact": "Myra Kovacek",
            "created_at": "2022-11-03, 14:20:51:563000",
            "last_updated": "2022-11-03, 14:20:51:563000"
        },
    ]
}


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


def test_should_have_(extraction_patched, latest_file_name_patched):
    extraction_patched.side_effect = [json.dumps(
        counterparty_test_data), json.dumps(location_test_data)]
    latest_file_name_patched.side_effect = [None, None]
    expected_columns = ['counterparty_id',
                        'counterparty_legal_name',
                        'counterparty_legal_address_line_1',
                        'counterparty_legal_address_line_2',
                        'counterparty_legal_district',
                        'counterparty_legal_city',
                        'counterparty_legal_postal_code',
                        'counterparty_legal_country',
                        'counterparty_legal_phone_number']
    assert list(create_dim_counterparty()[0].columns) == expected_columns


def test_should_have_correct_data_types(extraction_patched, latest_file_name_patched):
    extraction_patched.side_effect = [json.dumps(
        counterparty_test_data), json.dumps(location_test_data)]
    latest_file_name_patched.side_effect = [None, None]
    expected_data_types = {
        'counterparty_legal_name': 'object',
        'counterparty_legal_address_line_1': 'object',
        'counterparty_legal_address_line_2': 'object',
        'counterparty_legal_district': 'object',
        'counterparty_legal_city': 'object',
        'counterparty_legal_postal_code': 'object',
        'counterparty_legal_country': 'object',
        'counterparty_legal_phone_number': 'object'
    }
    data_frame = create_dim_counterparty()[0]
    for column, data_type in expected_data_types.items():
        assert data_frame[column].dtype == data_type
