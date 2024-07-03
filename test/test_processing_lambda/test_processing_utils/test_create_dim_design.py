from unittest.mock import patch
import json
from src.lambda_functions.processed_lambda import create_dim_design
import pytest

test_data = {
    "data": [
        {
            "design_id": 1,
            "created_at": "2022-11-03, 14:20:49:962000",
            "design_name": "Wooden",
            "file_location": "/lib",
            "file_name": "wooden-20201128-jdvi.json",
            "last_updated": "2022-11-03, 14:20:49:962000",
        },
        {
            "design_id": 2,
            "created_at": "2022-11-03, 14:20:49:962000",
            "design_name": "Steel",
            "file_location": "/etc/periodic",
            "file_name": "steel-20210725-fcxq.json",
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
    extraction_patched.return_value = json.dumps(test_data)
    latest_file_name_patched.return_value = None
    expected_columns = ["design_id", "design_name",
                        "file_location", "file_name"]
    assert list(create_dim_design()[0].columns) == expected_columns


def test_should_have_correct_data_types(extraction_patched, latest_file_name_patched):
    extraction_patched.return_value = json.dumps(test_data)
    latest_file_name_patched.return_value = None
    expected_data_types = {
        "design_name": "object",
        "file_location": "object",
        "file_name": "object",
    }
    data_frame = create_dim_design()[0]
    for column, data_type in expected_data_types.items():
        assert data_frame[column].dtype == data_type
