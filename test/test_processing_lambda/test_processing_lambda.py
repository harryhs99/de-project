import json
import boto3
from moto import mock_s3
import logging
from src.lambda_functions.processed_lambda import (
    extract_data_from_ingestion_s3,
    upload_to_processing_s3
)
import pytest
import os
import pandas as pd


logger = logging.getLogger('test')
logger.setLevel(logging.INFO)
logger.propagate = True

with open('test/test_processing_lambda/test_processing_utils/location_test_data.json') as f:
    test_data = json.load(f)


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-west-2'


@pytest.fixture(scope='function')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='eu-west-2')


@pytest.fixture
def ingestion_bucket(s3):
    s3.create_bucket(
        Bucket='ingestion_bucket',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    with open('test/test_processing_lambda/test_processing_utils/location_test_data.json') as f:
        address_data = json.load(f)
        json_data = json.dumps(address_data)
        s3.put_object(
            Body=json_data, Bucket='ingestion_bucket',
            Key='address.json'
        )


@pytest.fixture
def processed_bucket(s3):
    s3.create_bucket(
        Bucket='processed_bucket',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )


def test_processed_bucket_created_and_ingest_bucket(s3, ingestion_bucket, processed_bucket):
    result = s3.list_buckets()
    assert len(result["Buckets"]) == 2
    assert result["Buckets"][0]["Name"] == "ingestion_bucket"
    assert result["Buckets"][1]["Name"] == "processed_bucket"


def test_fake_ingestion_bucket_working(s3, ingestion_bucket):
    result = s3.list_objects_v2(Bucket='ingestion_bucket')
    assert len(result["Contents"]) == 1
    assert result["Contents"][0]["Key"] == "address.json"


def test_extract_from_ingestion_s3(s3, ingestion_bucket):
    expected_data = json.dumps(test_data)
    result = extract_data_from_ingestion_s3(
        'ingestion_bucket', 'address.json')
    assert result == expected_data


def test_should_handle_error_when_passed_invalid_key(s3, ingestion_bucket):
    with pytest.raises(Exception):
        extract_data_from_ingestion_s3(
            bucket='ingestion_bucket', key='no-key.json')


def test_should_handle_error_when_passed_invalid_bucket(s3, ingestion_bucket):
    with pytest.raises(Exception):
        extract_data_from_ingestion_s3(
            bucket='no-such-bucket', key='transaction.json')


def test_upload_to_processing_s3(s3, ingestion_bucket, processed_bucket):
    # mock create_dim_location function
    def create_dim_location(json_location):
        # setup location DataFrame
        df_location = pd.DataFrame(json_location['data'])
        # setup location columns
        dim_location = df_location[['address_id', 'address_line_1',
                                    'address_line_2', 'district', 'city',
                                    'postal_code', 'country', 'phone']]
        # rename columns
        dim_location.rename(
            columns={'address_id': 'location_id'}, inplace=True)
        dim_location.set_index('location_id', inplace=True)
        return dim_location

    # mimic what the lambda would do
    dim_location = create_dim_location(test_data)
    upload_to_processing_s3('processed_bucket',
                            'dim_location.parquet', dim_location)
    # tests
    result = s3.list_objects_v2(Bucket='processed_bucket')

    assert len(result["Contents"]) == 1
    assert result["Contents"][0]["Key"] == "dim_location.parquet"


def test_content_type_of_uploaded_file(s3, processed_bucket):
    def create_dim_location(json_location):
        # setup location DataFrame
        df_location = pd.DataFrame(json_location['data'])
        # setup location columns
        dim_location = df_location[['address_id', 'address_line_1',
                                    'address_line_2', 'district', 'city',
                                    'postal_code', 'country', 'phone']]
        # rename columns
        dim_location.rename(
            columns={'address_id': 'location_id'}, inplace=True)
        dim_location.set_index('location_id', inplace=True)
        return dim_location

    dim_location = create_dim_location(test_data)
    upload_to_processing_s3('processed_bucket',
                            'dim_location.parquet', dim_location)

    response = s3.head_object(Bucket='processed_bucket',
                              Key='dim_location.parquet')

    assert response['ContentType'] == 'binary/octet-stream'


def test_should_handle_if_error_for_upload_to_processing_s3(s3, processed_bucket):
    with pytest.raises(Exception):
        upload_to_processing_s3('processed_bucket', 'new-file', 'something')
