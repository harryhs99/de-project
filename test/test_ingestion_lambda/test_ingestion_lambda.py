import pytest
import os
import boto3
from unittest.mock import patch
from moto import mock_s3
from src.lambda_functions.ingestion_lambda import lambda_handler
import datetime


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
def setup_s3_bucket(s3):
    bucket_name = "test-183947598437"
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


@pytest.fixture
def mock_get_counterparty_data():
    with patch("src.lambda_functions.ingestion_lambda.get_counterparty_data",
               return_value=('''{
                   "data": [{"something": "something"}]}''', datetime.date.today())) as mock:
        yield mock


@pytest.fixture
def mock_get_address_data():
    with patch("src.lambda_functions.ingestion_lambda.get_address_data",
               return_value=('''{
                   "data": [{"something": "something"}]}''', datetime.date.today())) as mock:
        yield mock


@pytest.fixture
def mock_get_currency_util():
    with patch("src.lambda_functions.ingestion_lambda.get_currency_util",
               return_value=('''{
                   "data": [{"something": "something"}]}''', datetime.date.today())) as mock:
        yield mock


@pytest.fixture
def mock_get_department_data():
    with patch("src.lambda_functions.ingestion_lambda.get_department_data",
               return_value=('''{
                   "data": [{"something": "something"}]}''', datetime.date.today())) as mock:
        yield mock


@pytest.fixture
def mock_get_design_data():
    with patch("src.lambda_functions.ingestion_lambda.get_design_data",
               return_value=('''{
                   "data": [{"something": "something"}]}''', datetime.date.today())) as mock:
        yield mock


@pytest.fixture
def mock_get_payment_data():
    with patch("src.lambda_functions.ingestion_lambda.get_payment_data",
               return_value=('''{
                   "data": [{"something": "something"}]}''', datetime.date.today())) as mock:
        yield mock


@pytest.fixture
def mock_get_payment_type_data():
    with patch("src.lambda_functions.ingestion_lambda.get_payment_type_data",
               return_value=('''{
                   "data": [{"something": "something"}]}''', datetime.date.today())) as mock:
        yield mock


@pytest.fixture
def mock_get_purchase_order_data():
    with patch("src.lambda_functions.ingestion_lambda.get_purchase_order_data",
               return_value=('''{
                   "data": [{"something": "something"}]}''', datetime.date.today())) as mock:
        yield mock


@pytest.fixture
def mock_get_sales_order_util():
    with patch("src.lambda_functions.ingestion_lambda.get_sales_order_util",
               return_value=('''{
                   "data": [{"something": "something"}]}''', datetime.date.today())) as mock:
        yield mock


@pytest.fixture
def mock_get_staff_data():
    with patch("src.lambda_functions.ingestion_lambda.get_staff_data",
               return_value=('''{
                   "data": [{"something": "something"}]}''', datetime.date.today())) as mock:
        yield mock


@pytest.fixture
def mock_get_transaction_data():
    with patch("src.lambda_functions.ingestion_lambda.get_transaction_data",
               return_value=('''{
                   "data": [{"something": "something"}]}''', datetime.date.today())) as mock:
        yield mock


@pytest.fixture
def mock_get_secret():
    with patch("src.lambda_functions.ingestion_lambda.get_secret",
               return_value='''{"PGUSER": "nothing",
                "PGURL":"nothing", "PGDATABASE":"nothing",
                "PGPORT":"nothing", "PGPASSWORD": "nothing"}''') as mock:
        yield mock


@pytest.fixture
def mock_ingestion_bucket():
    with patch("src.lambda_functions.ingestion_lambda.ingestion_bucket",
               "test-183947598437") as mock:
        yield mock


@pytest.fixture
def mock_invalid_bucket():
    with patch("src.lambda_functions.ingestion_lambda.ingestion_bucket",
               "invalid-bucket") as mock:
        yield mock


@pytest.fixture
def mock_trigger_lambda_processed():
    with patch("src.lambda_functions.ingestion_lambda.trigger_lambda_processed",
               return_value=None) as mock:
        yield mock


def test_create_bucket(s3, setup_s3_bucket):
    result = s3.list_buckets()
    print(result["Buckets"])
    assert result["Buckets"][0]["Name"] == "test-183947598437"


def test_lambda_handler_ingests_all_data(s3, setup_s3_bucket, mock_ingestion_bucket,
                                         mock_get_counterparty_data, mock_get_address_data,
                                         mock_get_currency_util, mock_get_department_data,
                                         mock_get_design_data, mock_get_payment_data,
                                         mock_get_payment_type_data,
                                         mock_get_purchase_order_data, mock_get_sales_order_util,
                                         mock_get_staff_data, mock_get_transaction_data,
                                         mock_get_secret, mock_trigger_lambda_processed):
    list_of_bucket_keys = ['address', 'counterparty', 'currency',
                           'department', 'design', 'payment',
                           'payment_type', 'purchase_order', 'sales_order',
                           'staff', 'transaction']
    lambda_handler(None, None)
    result = s3.list_objects_v2(Bucket="test-183947598437")
    for key in result['Contents']:
        assert key['Key'].split('/')[0] in list_of_bucket_keys
    assert len(result['Contents']) == 11


def test_error_handling_in_lambda_handler(s3, setup_s3_bucket, mock_ingestion_bucket,
                                          mock_get_counterparty_data, mock_get_address_data,
                                          mock_get_currency_util, mock_get_department_data,
                                          mock_get_design_data, mock_get_payment_data,
                                          mock_get_payment_type_data,
                                          mock_get_purchase_order_data, mock_get_sales_order_util,
                                          mock_get_staff_data, mock_get_transaction_data,
                                          mock_get_secret, mock_invalid_bucket):
    with pytest.raises(Exception):
        lambda_handler(None, None)
