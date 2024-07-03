import pytest
import os
import boto3
from moto import mock_s3
from src.lambda_functions.warehouse_lambda import extract_data_from_processed_s3
from src.lambda_functions.warehouse_lambda import counterparty_util, currency_util
from src.lambda_functions.warehouse_lambda import design_util, staff_util, location_util
from unittest.mock import patch
import pandas as pd
import pg8000


user = "user123"
host = "localhost"
db = "test_tote_db"
password = "password123"
port = "5432"


file_path = "fake_data/processed_bucket/dim_counterparty.parquet"
with open(file_path, 'rb') as f:
    expected_counterparty_df = pd.read_parquet(f)

with open("fake_data/processed_bucket/dim_counterparty.parquet", 'rb') as f:
    df_counterparty = pd.read_parquet(f)

with open("fake_data/processed_bucket/dim_location.parquet", 'rb') as f:
    df_location = pd.read_parquet(f)

with open("fake_data/processed_bucket/dim_design.parquet", 'rb') as f:
    df_design = pd.read_parquet(f)

with open("fake_data/processed_bucket/dim_currency.parquet", 'rb') as f:
    df_currency = pd.read_parquet(f)

with open("fake_data/processed_bucket/dim_staff.parquet", 'rb') as f:
    df_staff = pd.read_parquet(f)


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture
def processed_bucket(s3):
    s3.create_bucket(
        Bucket="processed_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    with open("fake_data/processed_bucket/dim_counterparty.parquet", 'rb') as f:
        s3.put_object(
            Body=f, Bucket="processed_bucket", Key="dim_counterparty.parquet"
        )
    with open("fake_data/processed_bucket/dim_currency.parquet", 'rb') as f:
        s3.put_object(
            Body=f, Bucket="processed_bucket", Key="dim_currency.parquet"
        )
    with open("fake_data/processed_bucket/dim_date.parquet", 'rb') as f:
        s3.put_object(
            Body=f, Bucket="processed_bucket", Key="dim_date.parquet"
        )
    with open("fake_data/processed_bucket/dim_design.parquet", 'rb') as f:
        s3.put_object(
            Body=f, Bucket="processed_bucket", Key="dim_design.parquet"
        )
    with open("fake_data/processed_bucket/dim_location.parquet", 'rb') as f:
        s3.put_object(
            Body=f, Bucket="processed_bucket", Key="dim_location.parquet"
        )
    with open("fake_data/processed_bucket/dim_staff.parquet", 'rb') as f:
        s3.put_object(
            Body=f, Bucket="processed_bucket", Key="dim_staff.parquet"
        )
    with open("fake_data/processed_bucket/dim_transactions.parquet", 'rb') as f:
        s3.put_object(
            Body=f, Bucket="processed_bucket", Key="dim_transactions.parquet"
        )
    with open("fake_data/processed_bucket/fact_sales_order.parquet", 'rb') as f:
        s3.put_object(
            Body=f, Bucket="processed_bucket", Key="fact_sales_order.parquet"
        )


@pytest.fixture
def mock_extract_data_for_counterparty():
    with patch("src.lambda_functions.warehouse_lambda.extract_data_from_processed_s3", return_value=df_counterparty) as mock:
        yield mock


@pytest.fixture
def mock_extract_data_for_currency():
    with patch("src.lambda_functions.warehouse_lambda.extract_data_from_processed_s3", return_value=df_currency) as mock:
        yield mock


@pytest.fixture
def mock_extract_data_for_design():
    with patch("src.lambda_functions.warehouse_lambda.extract_data_from_processed_s3", return_value=df_design) as mock:
        yield mock


@pytest.fixture
def mock_extract_data_for_location():
    with patch("src.lambda_functions.warehouse_lambda.extract_data_from_processed_s3", return_value=df_location) as mock:
        yield mock


@pytest.fixture
def mock_extract_data_for_staff():
    with patch("src.lambda_functions.warehouse_lambda.extract_data_from_processed_s3", return_value=df_staff) as mock:
        yield mock


@pytest.fixture
def mock_get_latest_file_name():
    with patch("src.lambda_functions.warehouse_lambda.get_latest_file_name", return_value=None) as mock:
        yield mock


@pytest.fixture
def mock_warehouse_update():
    with patch("src.lambda_functions.warehouse_lambda.warehouse_update", return_value=(None, False)) as mock:
        yield mock


def test_processed_bucket_created(s3, processed_bucket):
    result = s3.list_buckets()
    assert len(result["Buckets"]) == 1
    assert result["Buckets"][0]["Name"] == "processed_bucket"


def test_all_objects_in_processed_bucket(s3, processed_bucket):
    result = s3.list_objects_v2(Bucket="processed_bucket")
    expected_keys = ['dim_counterparty.parquet',
                     'dim_currency.parquet',
                     'dim_date.parquet',
                     'dim_design.parquet',
                     'dim_location.parquet',
                     'dim_staff.parquet',
                     'dim_transactions.parquet',
                     'fact_sales_order.parquet']
    for key in result["Contents"]:
        assert key["Key"] in expected_keys


def test_extract_data_from_processed(s3, processed_bucket):
    result = extract_data_from_processed_s3(
        "processed_bucket", "dim_counterparty.parquet"
    )
    print(result)
    assert result.equals(expected_counterparty_df)


def test_counter_party_util(s3, processed_bucket, mock_extract_data_for_counterparty,
                            mock_warehouse_update, mock_get_latest_file_name):
    expected_names = ['XYZ Corporation', 'ABC Industries',
                      '123 Manufacturing', 'Sample Corporation',
                      'Test Corp']
    counterparty_util(user, host, db, port, password)
    conn = pg8000.native.Connection(
        user=user, host=host, database=db, port=port, password=password)
    query = '''SELECT * FROM dim_counterparty;'''
    result = conn.run(query)
    conn.close()
    for item in result:
        assert item[1] in expected_names


def test_currency_util(s3, processed_bucket, mock_extract_data_for_currency,
                       mock_warehouse_update, mock_get_latest_file_name):
    expected_result = [[1, 'GBP', 'pound'],
                       [2, 'USD', 'dollar'],
                       [3, 'EUR', 'euro']]
    conn = pg8000.native.Connection(
        user=user, host=host, database=db, port=port, password=password)
    query = '''DELETE FROM dim_currency;'''
    conn.run(query)
    currency_util(user, host, db, port, password)
    query = '''SELECT * FROM dim_currency;'''
    result = conn.run(query)
    print(result)
    conn.close()
    assert result == expected_result


def test_design_util(s3, processed_bucket, mock_extract_data_for_design,
                     mock_warehouse_update, mock_get_latest_file_name):
    expected_names = ['Test Wooden', 'Test Steel',
                      'Test Steel', 'Test Granite',
                      'Test Fresh', 'Test Wooden',
                      'Test Concrete', 'Test Soft']
    conn = pg8000.native.Connection(
        user=user, host=host, database=db, port=port, password=password)
    query = '''DELETE FROM dim_design;'''
    conn.run(query)
    design_util(user, host, db, port, password)
    query = '''SELECT * FROM dim_design;'''
    result = conn.run(query)
    conn.close()
    for item in result:
        assert item[1] in expected_names


def test_location_util(s3, processed_bucket, mock_extract_data_for_location,
                       mock_warehouse_update, mock_get_latest_file_name):
    expected_addresses = ['123 Main Street', '456 Elm Avenue', '789 Oak Lane',
                          '246 Maple Street', '135 Pine Road']
    conn = pg8000.native.Connection(
        user=user, host=host, database=db, port=port, password=password)
    query = '''DELETE FROM dim_location;'''
    conn.run(query)
    location_util(user, host, db, port, password)
    query = '''SELECT * FROM dim_location;'''
    result = conn.run(query)
    print(result)
    conn.close()
    for item in result:
        assert item[1] in expected_addresses


def test_staff_util(s3, processed_bucket, mock_extract_data_for_staff,
                    mock_warehouse_update, mock_get_latest_file_name):
    expected_names = ['John', 'Jane', 'Michael', 'Emily',
                      'Robert', 'Jessica', 'David', 'Sarah',
                      'James', 'Jennifer']
    conn = pg8000.native.Connection(
        user=user, host=host, database=db, port=port, password=password)
    query = '''DELETE FROM dim_staff;'''
    conn.run(query)
    staff_util(user, host, db, port, password)
    query = '''SELECT * FROM dim_staff;'''
    result = conn.run(query)
    print(result)
    conn.close()
    for item in result:
        assert item[1] in expected_names
