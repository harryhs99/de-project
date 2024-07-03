from unittest.mock import patch
import json
from src.lambda_functions.processed_lambda import create_dim_staff
import pytest

test_staff_data = {
    "data": [
        {
            "staff_id": 1,
            "first_name": "Jeremie",
            "last_name": "Franey",
            "department_id": 2,
            "email_address": "jeremie.franey@terrifictotes.com",
            "created_at": "2022-11-03, 14:20:51:563000",
            "last_updated": "2022-11-03, 14:20:51:563000",
        },
        {
            "staff_id": 2,
            "first_name": "Deron",
            "last_name": "Beier",
            "department_id": 6,
            "email_address": "deron.beier@terrifictotes.com",
            "created_at": "2022-11-03, 14:20:51:563000",
            "last_updated": "2022-11-03, 14:20:51:563000",
        },
    ]
}


test_department_data = {
    "data": [
        {
            "department_id": 2,
            "department_name": "Purchasing",
            "location": "Manchester",
            "manager": "Naomi Lapaglia",
            "created_at": "2022-11-03, 14:20:49:962000",
            "last_updated": "2022-11-03, 14:20:49:962000",
        },
        {
            "department_id": 6,
            "department_name": "Facilities",
            "location": "Manchester",
            "manager": "Shelley Levene",
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


def test_should_have_correct_columns(extraction_patched, latest_file_name_patched):
    extraction_patched.side_effect = [json.dumps(
        test_department_data), json.dumps(test_staff_data)]
    latest_file_name_patched.side_effect = [None, None]
    expected_columns = ['staff_id', 'first_name', 'last_name',
                        'email_address', 'department_name', 'location']
    assert list(create_dim_staff()[0].columns) == expected_columns


def test_should_have_correct_data_types(extraction_patched, latest_file_name_patched):
    extraction_patched.side_effect = [json.dumps(
        test_department_data), json.dumps(test_staff_data)]
    latest_file_name_patched.side_effect = [None, None]
    expected_data_types = {
        "first_name": "object",
        "last_name": "object",
        "email_address": "object",
        "department_name": "object",
        "location": "object",
    }
    data_frame = create_dim_staff()[0]
    for column, data_type in expected_data_types.items():
        assert data_frame[column].dtype == data_type
