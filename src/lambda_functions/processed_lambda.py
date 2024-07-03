import boto3
import pandas as pd
import logging
import io
import json
from botocore.exceptions import ClientError
import time
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """This function, lambda_handler, is the entry point for an AWS Lambda
    function designed to process an S3 event triggered by the upload of a file.
    It handles the processing of a specific file named "transaction.json". The
    function performs several tasks related to data transformation and loading.
    The processed data is stored in Parquet format in an S3 bucket named
    "processed-bucket-1158020804995033".

    Parameters:
    event (dict): A dictionary representing the event data passed to the Lambda function.
    It contains information about the S3 event that triggered this function.
    context (object): An object provided by the AWS Lambda runtime containing information
    about the execution environment and function execution.

    Returns:
    None: The function does not return any value.

    Raises:
    ClientError: If an error occurs while interacting with the AWS S3 service.
    Exception: If any other unexpected error occurs during the data processing.
    """
    s3_bucket_name, s3_object_name = get_object_path(event["Records"])

    ingestion_bucket = "ingestion-bucket-1158020804995033"
    processing_bucket = "processed-bucket-1158020804995033"

    if s3_object_name == "processed_trigger.txt":
        try:
            parquet_func = {'dim_currency': create_dim_currency,
                            'dim_counterparty': create_dim_counterparty,
                            'dim_design': create_dim_design,
                            'dim_location': create_dim_location,
                            'dim_staff': create_dim_staff,
                            'dim_transaction': create_dim_transaction,
                            'dim_payment_type': create_dim_payment_type,
                            'fact_sales_order': create_fact_sales_order,
                            'fact_purchase_order': create_fact_purchase_order,
                            'fact_payment': create_fact_payment
                            }

            for table_name, create_func in parquet_func.items():
                start_time = time.time()
                data, file_name = create_func(ingestion_bucket)
                if not data.empty:
                    upload_to_processing_s3(processing_bucket, file_name, data)
                    end_time = time.time()
                    logger.info(f"{file_name} saved to S3:\
                      {end_time - start_time} seconds to execute.")
                else:
                    end_time = time.time()
                    logger.info(f"{file_name} no update necessary:\
                      {end_time - start_time} seconds to execute.")
                    pass
            if table_name == 'fact_payment':
                trigger_lambda_warehouse()

        except ClientError as e:
            logger.error(f"An S3 Error occurred: {e}")
        except Exception as e:
            logger.error(f"An Error has occurred: {e}")
            raise e


def get_object_path(records):
    """Extracts bucket and object references from Records field of event.

    Parameters:
    records (list): A list of records representing the event data passed to the function.
    Each record contains information about the S3 event, and this function expects the S3
    bucket and object details to be present in the first record.

    Returns:
    tuple: A tuple containing two elements - the S3 bucket name (str) and the S3 object key (str).
    """
    return records[0]["s3"]["bucket"]["name"], records[0]["s3"]["object"]["key"]


def get_latest_file_name(bucket_name, prefix):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    dates = []
    file_names = []
    for obj in response.get('Contents', []):
        key = obj['Key']
        file_name = key.split('/')[-1].split('.')[0]
        date_string = file_name.split('.')[0]
        date = datetime.strptime(date_string, '%Y-%m-%d-%H-%M-%S')
        dates.append(date)
        file_names.append(file_name)

    if dates:
        latest_date_index = dates.index(max(dates))
        latest_file_name = file_names[latest_date_index]
        return latest_file_name
    else:
        print('hey i should not happen')
        return None


def extract_data_from_ingestion_s3(bucket, key):
    """Retrieves JSON data from the specified S3 bucket and object key.

    Args:
        bucket (str): The name of the S3 bucket from which to retrieve the JSON data.
        key (str): The key (i.e., path) of the S3 object containing the JSON data.

    Returns:
        dict: A Python dictionary containing the decoded JSON data retrieved from the
        specified S3 object.

    Raises:
        ClientError: If an error occurs while interacting with the AWS S3 service, such as
        invalid credentials or insufficient permissions.
        Exception: If any other unexpected error occurs during the data extraction process.
    """

    s3_client = boto3.client("s3")
    try:
        res = s3_client.get_object(Bucket=bucket, Key=key)
        json_data = res["Body"].read().decode()
        return json_data
    except ClientError as e:
        logger.error(f"An error occurred while retrieving data from S3: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e


def upload_to_processing_s3(destination_bucket, destination_key, file_contents):
    """
    Uploads the provided file contents as a Parquet object to the specified S3 bucket.

    Args:
        destination_bucket (str): The name of the destination S3 bucket.
        destination_key (str): The key (path) of the object within the destination bucket.
        file_contents (pandas.DataFrame): The file contents to be uploaded as a Parquet object.

    Raises:
        ClientError: If there is an issue while interacting with the S3 client.
        Exception: For other unexpected errors during the upload process.

    Returns:
        None: The function does not return anything explicitly, but raises exceptions when
        necessary.
    """
    s3_client = boto3.client("s3")

    try:
        s3_client.head_object(Bucket=destination_bucket, Key=destination_key)
        logger.info(
            f"File {destination_key} already exists in bucket {destination_bucket}. Skipping upload.")

    except ClientError as e:

        if e.response["Error"]["Code"] == "404":
            try:
                out_buffer = io.BytesIO()
                file_contents.to_parquet(out_buffer, index=False)
                s3_client.put_object(
                    Bucket=destination_bucket, Key=destination_key, Body=out_buffer.getvalue()
                )
                logger.info(
                    f"File {destination_key} uploaded to bucket {destination_bucket}.")
            except ClientError as e:
                logger.error(
                    f"An error occurred while uploading data to S3: {e}")
                raise e
            except Exception as e:
                logger.error(f"An error occurred: {e}")
                raise e
        else:
            logger.error(
                f"An error occurred while checking if file exists in S3: {e}")
            raise e


def trigger_lambda_warehouse():
    s3 = boto3.client('s3')
    try:
        res = s3.get_object(
            Bucket="processed-bucket-1158020804995033", Key='warehouse_trigger.txt')
        data = res['Body'].read().decode('utf-8')
        s3.put_object(Bucket="processed-bucket-1158020804995033",
                      Key='warehouse_trigger.txt', Body=data)
        print('\nlambda 2 trigger........\n')
    except ClientError as e:
        logger.error(
            f"An error occurred while retrieving warehouse_trigger.txt from S3: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e


def processed_update(table_name, latest):

    s3 = boto3.client('s3')
    try:
        res = s3.get_object(
            Bucket="processed-bucket-1158020804995033", Key='processed_update.json')
        data = res['Body'].read().decode('utf-8')
        data = json.loads(data)
        if data[table_name] == "null":
            data[table_name] = latest
            skip = False
        else:
            new_file_date = datetime.strptime(latest, '%Y-%m-%d-%H-%M-%S')
            json_date = datetime.strptime(
                data[table_name], '%Y-%m-%d-%H-%M-%S')
            if new_file_date > json_date:
                data[table_name] = new_file_date.strftime('%Y-%m-%d-%H-%M-%S')
                skip = False
            else:
                skip = True
        new_file_name = data[table_name]
        data = json.dumps(data)
        s3.put_object(Bucket="processed-bucket-1158020804995033",
                      Key='processed_update.json', Body=data)
        return new_file_name, skip
    except ClientError as e:
        logger.error(
            f"An error occurred while retrieving warehouse_update.json from S3: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e


def create_dim_currency(bucket_name=None):
    """Creates a dimension table for currency data.

    The function extracts currency data from the ingestion S3 bucket, processes it, and returns
    a pandas DataFrame containing relevant currency information.

    Returns:
        pandas.DataFrame: A dimension table containing currency data with the following columns:
            - currency_name (str): The name of the currency.
            - currency_code (str): The currency code (e.g., USD, EUR, GBP).
    Raises:
        Exception: If there is an error while creating the dim_currency DataFrame.
            This could be due to issues with data extraction or processing.
    """
    try:
        latest = get_latest_file_name(bucket_name, "currency")
        latest, skip = processed_update('currency', latest)
        key = f'currency/{latest}.json'
        key_parquet = f'dim_currency/{latest}.parquet'

        if skip:
            return pd.DataFrame(), key_parquet
        else:
            json_currency = extract_data_from_ingestion_s3(
                bucket="ingestion-bucket-1158020804995033", key=key
            )
            json_currency = json.loads(json_currency)
            df_currency = pd.DataFrame(json_currency["data"])
            dim_currency = df_currency.iloc[:, :2]
            dim_currency["currency_name"] = ["pound", "dollar", "euro"]
            return dim_currency, key_parquet
    except Exception as e:
        logger.error(
            f"An error occurred while creating the dim_currency DataFrame: {e}"
        )


def create_dim_design(bucket_name=None):
    """Extracts design data from a specified S3 bucket, parses the data into a
    DataFrame, and creates a dimension table "dim_design" with specific
    columns. If any error occurs during this process, the error will be logged.

    The design data must be a json object with a "data" key. The "data" value is expected to
    be a list of dictionaries, each dictionary corresponding to a row in the DataFrame and each
    key-value pair within the dictionary corresponding to a column and its value.

    The DataFrame is expected to include the following columns: "design_id", "design_name",
    "file_location", and "file_name". The "design_id" column is used as the index.

    The function will return the created dimension table.

    Returns:
        pd.DataFrame: The dimension design DataFrame. The index of the DataFrame is "design_id",
        and columns are "design_name", "file_location", and "file_name".

    Raises:
        Exception: If any error occurs while creating the dim_design DataFrame, an error
        will be logged and the exception will be raised.
    """
    try:
        latest = get_latest_file_name(bucket_name, "design")
        latest, skip = processed_update('design', latest)
        key = f'design/{latest}.json'
        key_parquet = f'dim_design/{latest}.parquet'

        if skip:
            return pd.DataFrame(), key_parquet
        else:
            json_design = extract_data_from_ingestion_s3(
                bucket="ingestion-bucket-1158020804995033", key=key
            )
            json_design = json.loads(json_design)
            df_design = pd.DataFrame(json_design["data"])
            dim_design = df_design[
                ["design_id", "design_name", "file_location", "file_name"]
            ]
            return dim_design, key_parquet
    except Exception as e:
        logger.error(
            f"An error occurred while creating the dim_design DataFrame: {e}")


def create_dim_staff(bucket_name=None):
    """Extracts both department and staff data from specified S3 buckets,
    parses the data into respective DataFrames, merges them using a left join
    on "department_id", and creates a dimension table "dim_staff". Drops
    unnecessary columns from the merged DataFrame. If any error occurs during
    this process, the error will be logged.

    Both department and staff data must be json objects with a "data" key. The "data" value is
    expected to be a list of dictionaries, each dictionary corresponding to a row in the
    DataFrame and each key-value pair within the dictionary corresponding to a column and its
    value.

    The resulting DataFrame includes all columns from both the staff and department DataFrames,
    excluding "department_id", "created_at", and "last_updated". The "staff_id" column is used
    as the index.

    The function will return the created dimension table.

    Returns:
        pd.DataFrame: The dimension staff DataFrame. The index of the DataFrame is "staff_id",
        and includes all other columns from the department and staff DataFrames excluding
        "department_id", "created_at", and "last_updated".

    Raises:
        Exception: If any error occurs while creating the dim_staff DataFrame, an error
        will be logged and the exception will be raised.

    Example:
        >>> dim_staff = create_dim_staff()
    """

    try:
        latest_department = get_latest_file_name(bucket_name, "department")
        latest_department, skip = processed_update(
            'department', latest_department)
        key_department = f'department/{latest_department}.json'

        latest_staff = get_latest_file_name(bucket_name, "staff")
        latest_staff, skip2 = processed_update('staff', latest_staff)
        key_staff = f'staff/{latest_staff}.json'
        key_parquet = f'dim_staff/{latest_staff}.parquet'

        if skip and skip2:
            return pd.DataFrame(), key_parquet
        else:
            json_department = extract_data_from_ingestion_s3(
                bucket="ingestion-bucket-1158020804995033", key=key_department
            )
            key_parquet = f'dim_staff/{latest_staff}.parquet'
            json_staff = extract_data_from_ingestion_s3(
                bucket="ingestion-bucket-1158020804995033", key=key_staff
            )
            json_department = json.loads(json_department)
            df_department = pd.DataFrame(json_department["data"])
            json_staff = json.loads(json_staff)
            df_staff1 = pd.DataFrame(json_staff["data"])
            df_staff = df_staff1.merge(
                df_department[["department_id",
                               "department_name", "location"]],
                on="department_id",
                how="left",
            )

            dim_staff = df_staff.drop(
                ["department_id", "created_at", "last_updated"], axis=1
            )
            return dim_staff, key_parquet
    except Exception as e:
        logger.error(
            f"An error occurred while creating the dim_staff DataFrame: {e}")


def create_dim_location(bucket_name=None):
    """Extracts location data from a specified S3 bucket, parses the data into
    a DataFrame, and creates a dimension table "dim_location" with specific
    columns. If any error occurs during this process, the error will be logged.

    The location data must be a json object with a "data" key. The "data" value is expected to
    be a list of dictionaries, each dictionary corresponding to a row in the DataFrame and each
    key-value pair within the dictionary corresponding to a column and its value.

    The DataFrame is expected to include the following columns: "address_id", "address_line_1",
    "address_line_2", "district", "city", "postal_code", "country", and "phone". The "address_id"
    column is renamed to "location_id" and is used as the index.

    The function will return the created dimension table.

    Returns:
        pd.DataFrame: The dimension location DataFrame.
        The index of the DataFrame is "location_id",
        and columns are "address_line_1", "address_line_2", "district", "city", "postal_code",
        "country", and "phone".

    Raises:
        Exception: If any error occurs while creating the dim_location DataFrame, an error
        will be logged and the exception will be raised.

    Example:
        >>> dim_location = create_dim_location()
    """

    try:
        latest = get_latest_file_name(bucket_name, "address")
        latest, skip = processed_update('address', latest)
        key = f'address/{latest}.json'
        key_parquet = f'dim_location/{latest}.parquet'
        if skip:
            return pd.DataFrame(), key_parquet
        else:
            json_location = extract_data_from_ingestion_s3(
                bucket="ingestion-bucket-1158020804995033", key=key
            )
            json_location = json.loads(json_location)
            df_location = pd.DataFrame(json_location["data"])
            dim_location = df_location[
                [
                    "address_id",
                    "address_line_1",
                    "address_line_2",
                    "district",
                    "city",
                    "postal_code",
                    "country",
                    "phone",
                ]
            ]
            dim_location = dim_location.rename(
                columns={"address_id": "location_id"})
            return dim_location, key_parquet
    except Exception as e:
        logger.error(
            f"An error occurred while creating the dim_location DataFrame: {e}"
        )


def create_dim_counterparty(bucket_name=None):
    """Extracts both counterparty and location data from specified S3 buckets,
    parses the data into respective DataFrames, merges them using a left join
    on "legal_address_id" and "address_id" respectively, and creates a
    dimension table "dim_counterparty". If any error occurs during this
    process, the error will be logged.

    Both counterparty and location data must be json objects with a "data" key. The "data" value
    is expected to be a list of dictionaries, each dictionary corresponding to a row in the
    DataFrame and each key-value pair within the dictionary corresponding to a column and its
    value.

    The resulting DataFrame includes all columns from both the counterparty and location
    DataFrames, with certain columns from the location DataFrame being renamed to match the
    counterparty schema. The "counterparty_id" column is used as the index.

    The function will return the created dimension table.

    Returns:
        pd.DataFrame: The dimension counterparty DataFrame. The index of the DataFrame is
        "counterparty_id", and includes all other columns from the counterparty and location
        DataFrames, with certain location columns being renamed to match the counterparty schema.

    Raises:
        Exception: If any error occurs while creating the dim_counterparty DataFrame, an error
        will be logged and the exception will be raised.

    Example:
        >>> dim_counterparty = create_dim_counterparty()
    """

    try:
        latest_counterparty = get_latest_file_name(bucket_name, "counterparty")
        latest_counterparty, skip = processed_update(
            'counterparty', latest_counterparty)
        key_parquet = f'dim_counterparty/{latest_counterparty}.parquet'
        key_counterparty = f'counterparty/{latest_counterparty}.json'

        latest_address = get_latest_file_name(bucket_name, "address")
        latest_address, skip2 = processed_update('address', latest_address)
        key_address = f'address/{latest_address}.json'

        if skip and skip2:
            return pd.DataFrame(), key_parquet
        else:
            json_counterparty = extract_data_from_ingestion_s3(
                bucket="ingestion-bucket-1158020804995033", key=key_counterparty
            )

            json_location = extract_data_from_ingestion_s3(
                bucket="ingestion-bucket-1158020804995033", key=key_address
            )
            json_counterparty = json.loads(json_counterparty)
            df_counterparty = pd.DataFrame(json_counterparty["data"])
            json_location = json.loads(json_location)
            df_location = pd.DataFrame(json_location["data"])
            dim_counterparty = pd.merge(
                df_counterparty,
                df_location,
                left_on="legal_address_id",
                right_on="address_id",
                how="left",
            )
            dim_counterparty = dim_counterparty[
                [
                    "counterparty_id",
                    "counterparty_legal_name",
                    "address_line_1",
                    "address_line_2",
                    "district",
                    "city",
                    "postal_code",
                    "country",
                    "phone",
                ]
            ]

            dim_counterparty = dim_counterparty.rename(
                columns={
                    "address_line_1": "counterparty_legal_address_line_1",
                    "address_line_2": "counterparty_legal_address_line_2",
                    "district": "counterparty_legal_district",
                    "city": "counterparty_legal_city",
                    "postal_code": "counterparty_legal_postal_code",
                    "country": "counterparty_legal_country",
                    "phone": "counterparty_legal_phone_number",
                }
            )
            return dim_counterparty, key_parquet
    except Exception as e:
        logger.error(
            f"An error occurred while creating the dim_counterparty DataFrame: {e}"
        )


def create_dim_date(bucket_name=None):
    """Creates a dimension DataFrame with various time-related columns.

    Returns:
        pandas.DataFrame: A DataFrame containing the dimension data for dates.

    Raises:
        Exception: If an error occurs during the creation process.
    """
    try:
        date_range = pd.date_range(
            start="2022-01-01", end="2023-12-31", freq="D")
        dim_date = pd.date_range(
            start='2022-01-01', end='2023-12-31', freq='D')
        dim_date = pd.DataFrame(date_range, columns=['date_id'])
        dim_date['year'] = dim_date['date_id'].dt.year
        dim_date['month'] = dim_date['date_id'].dt.month
        dim_date['day'] = dim_date['date_id'].dt.day
        dim_date['day_of_week'] = dim_date['date_id'].dt.weekday
        dim_date['day_name'] = dim_date['date_id'].dt.strftime('%A')
        dim_date['month_name'] = dim_date['date_id'].dt.strftime('%B')
        dim_date['quarter'] = dim_date['date_id'].dt.to_period('Q')
        return dim_date
    except Exception as e:
        logger.error(
            f"An error occurred while creating the dim_date DataFrame: {e}")


def create_dim_transaction(bucket_name=None):
    """Creates a dimension table for transactions from JSON data.

    This function extracts transaction data from the ingestion S3 bucket,
    converts it into a DataFrame, and then processes it to create the dimension
    table `dim_transaction`. The DataFrame contains relevant transaction information
    and excludes the 'created_at' and 'last_updated' columns.

    Returns:
        pandas.DataFrame: The dimension table `dim_transaction` containing the
        processed transaction data.

    Raises:
        Exception: If any error occurs during the data extraction or processing,
        it will be logged and raised as an exception.

    Example:
        dim_transaction = create_dim_transaction()
    """

    try:
        latest = get_latest_file_name(bucket_name, "transaction")
        latest, skip = processed_update('transaction', latest)
        key = f'transaction/{latest}.json'
        key_parquet = f'dim_transaction/{latest}.parquet'
        if skip:
            return pd.DataFrame(), key_parquet
        else:
            json_transaction_data = extract_data_from_ingestion_s3(
                bucket="ingestion-bucket-1158020804995033", key=key
            )
            json_transaction_data = json.loads(json_transaction_data)
            df_transaction = pd.DataFrame(json_transaction_data["data"])
            dim_transaction = df_transaction.drop(
                ["created_at", "last_updated"], axis=1)
            dim_transaction[["sales_order_id", "purchase_order_id"]] = (
                dim_transaction[["sales_order_id", "purchase_order_id"]]
                .fillna(0)
                .astype(int)
            )
            return dim_transaction, key_parquet
    except Exception as e:
        logger.error(
            f"An error occurred while creating the dim_transaction DataFrame: {e}"
        )


def create_dim_payment_type(bucket_name=None):

    try:
        latest = get_latest_file_name(bucket_name, "payment_type")
        latest, skip = processed_update('payment_type', latest)
        key = f'payment_type/{latest}.json'
        key_parquet = f'dim_payment_type/{latest}.parquet'
        if skip:
            return pd.DataFrame(), key_parquet
        else:
            json_payment_type_data = extract_data_from_ingestion_s3(
                bucket="ingestion-bucket-1158020804995033", key=key
            )
            json_payment_type_data = json.loads(json_payment_type_data)
            df_payment_type = pd.DataFrame(json_payment_type_data["data"])
            dim_payment_type = df_payment_type.drop(
                ["created_at", "last_updated"], axis=1)
            return dim_payment_type, key_parquet
    except Exception as e:
        logger.error(
            f"An error occurred while creating the dim_payment_type DataFrame: {e}"
        )


def create_fact_sales_order(bucket_name=None):
    """Creates a fact table for sales orders from JSON data.

    This function extracts sales order data from the ingestion S3 bucket,
    converts it into a DataFrame, and then processes it to create the fact
    table `fact_sales_order`. The DataFrame contains relevant sales order information,
    and specific columns are processed to have appropriate data types.

    Returns:
        pandas.DataFrame: The fact table `fact_sales_order` containing the
        processed sales order data.

    Raises:
        Exception: If any error occurs during the data extraction or processing,
        it will be logged and raised as an exception.

    Example:
        fact_sales_order = create_fact_sales_order()
    """

    try:
        latest = get_latest_file_name(bucket_name, "sales_order")
        latest, skip = processed_update('sales_order', latest)
        key = f'sales_order/{latest}.json'
        key_parquet = f'fact_sales_order/{latest}.parquet'
        if skip:
            return pd.DataFrame(), key_parquet
        else:
            json_sales_order = extract_data_from_ingestion_s3(
                bucket="ingestion-bucket-1158020804995033", key=key
            )
            json_sales_order = json.loads(json_sales_order)
            df_sales_order = pd.DataFrame(json_sales_order["data"])
            df_sales_order[["created_date", "created_time"]] = df_sales_order[
                "created_at"
            ].str.split(", ", expand=True)
            df_sales_order[["last_updated_date", "last_updated_time"]] = df_sales_order[
                "last_updated"
            ].str.split(", ", expand=True)
            fact_sales_order = df_sales_order[
                [
                    "sales_order_id",
                    "created_date",
                    "created_time",
                    "last_updated_date",
                    "last_updated_time",
                    "staff_id",
                    "counterparty_id",
                    "units_sold",
                    "unit_price",
                    "currency_id",
                    "design_id",
                    "agreed_payment_date",
                    "agreed_delivery_date",
                    "agreed_delivery_location_id",
                ]
            ].copy()
            fact_sales_order["unit_price"] = pd.to_numeric(
                fact_sales_order["unit_price"])
            fact_sales_order["created_date"] = pd.to_datetime(
                fact_sales_order["created_date"]
            )
            fact_sales_order["created_time"] = (
                pd.to_datetime(
                    fact_sales_order["created_time"], format="%H:%M:%S:%f")
                .dt.floor("s")
                .dt.time
            )
            fact_sales_order["last_updated_date"] = pd.to_datetime(
                fact_sales_order["last_updated_date"]
            )
            fact_sales_order["last_updated_time"] = (
                pd.to_datetime(
                    fact_sales_order["last_updated_time"], format="%H:%M:%S:%f")
                .dt.floor("s")
                .dt.time
            )
            fact_sales_order["agreed_payment_date"] = pd.to_datetime(
                fact_sales_order["agreed_payment_date"]
            )
            fact_sales_order["agreed_delivery_date"] = pd.to_datetime(
                fact_sales_order["agreed_delivery_date"]
            )

            return fact_sales_order, key_parquet
    except Exception as e:
        logger.error(
            f"An error occurred while creating the fact_sales DataFrame: {e}")


def create_fact_purchase_order(bucket_name=None):
    try:
        latest = get_latest_file_name(bucket_name, "purchase_order")
        latest, skip = processed_update('purchase_order', latest)
        key = f'purchase_order/{latest}.json'
        key_parquet = f'fact_purchase_order/{latest}.parquet'
        if skip:
            return pd.DataFrame(), key_parquet
        else:
            json_purchase_order = extract_data_from_ingestion_s3(
                bucket='ingestion-bucket-1158020804995033', key=key)
            json_purchase_order = json.loads(json_purchase_order)
            df_purchase_order = pd.DataFrame(json_purchase_order['data'])
            df_purchase_order[['created_date', 'created_time']
                              ] = df_purchase_order['created_at'].str.split(', ', expand=True)
            df_purchase_order[['last_updated_date', 'last_updated_time']
                              ] = df_purchase_order['last_updated'].str.split(', ', expand=True)
            fact_purchase_order = df_purchase_order[[
                'purchase_order_id',
                'created_date',
                'created_time',
                'last_updated_date',
                'last_updated_time',
                'staff_id',
                'counterparty_id',
                'item_code',
                'item_quantity',
                'item_unit_price',
                'currency_id',
                'agreed_delivery_date',
                'agreed_payment_date',
                'agreed_delivery_location_id']].copy()
            fact_purchase_order['item_unit_price'] = pd.to_numeric(
                fact_purchase_order['item_unit_price'])
            fact_purchase_order['created_date'] = pd.to_datetime(
                fact_purchase_order['created_date'])
            fact_purchase_order['created_time'] = pd.to_datetime(
                fact_purchase_order['created_time'], format='%H:%M:%S:%f').dt.floor('s').dt.time
            fact_purchase_order['last_updated_date'] = pd.to_datetime(
                fact_purchase_order['last_updated_date'])
            fact_purchase_order['last_updated_time'] = pd.to_datetime(
                fact_purchase_order['last_updated_time'], format='%H:%M:%S:%f').dt.floor('s').dt.time
            fact_purchase_order['agreed_payment_date'] = pd.to_datetime(
                fact_purchase_order['agreed_payment_date'])
            fact_purchase_order['agreed_delivery_date'] = pd.to_datetime(
                fact_purchase_order['agreed_delivery_date'])
            return fact_purchase_order, key_parquet
    except Exception as e:
        logger.error(
            f"An error occurred while creating the fact_purchase_order DataFrame: {e}"
        )


def create_fact_payment(bucket_name=None):

    try:
        latest = get_latest_file_name(bucket_name, "payment")
        latest, skip = processed_update('payment', latest)
        key = f'payment/{latest}.json'
        key_parquet = f'fact_payment/{latest}.parquet'
        if skip:
            return pd.DataFrame(), key_parquet
        else:
            json_payment = extract_data_from_ingestion_s3(
                bucket='ingestion-bucket-1158020804995033', key=key)
            json_payment = json.loads(json_payment)
            df_payment = pd.DataFrame(json_payment['data'])
            df_payment[['created_date', 'created_time']
                       ] = df_payment['created_at'].str.split(', ', expand=True)
            df_payment[['last_updated_date', 'last_updated']
                       ] = df_payment['last_updated'].str.split(', ', expand=True)
            fact_payment = df_payment[[
                'payment_id',
                'created_date',
                'created_time',
                'last_updated_date',
                'last_updated',
                'transaction_id',
                'counterparty_id',
                'payment_amount',
                'currency_id',
                'payment_type_id',
                'paid',
                'payment_date']].copy()
            fact_payment['payment_amount'] = pd.to_numeric(
                fact_payment['payment_amount'])
            fact_payment['created_date'] = pd.to_datetime(
                fact_payment['created_date'])
            fact_payment['created_time'] = pd.to_datetime(
                fact_payment['created_time'], format='%H:%M:%S:%f').dt.floor('s').dt.time
            fact_payment['last_updated_date'] = pd.to_datetime(
                fact_payment['last_updated_date'])
            fact_payment['last_updated'] = pd.to_datetime(
                fact_payment['last_updated'], format='%H:%M:%S:%f').dt.floor('s').dt.time
            fact_payment['payment_date'] = pd.to_datetime(
                fact_payment['payment_date'])
            return fact_payment, key_parquet
    except Exception as e:
        logger.error(
            f"An error occurred while creating the fact_payment DataFrame: {e}"
        )
