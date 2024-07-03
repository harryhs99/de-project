import logging
import boto3
import pg8000.native
from botocore.exceptions import ClientError
import pandas as pd
import json
import io
import time
from datetime import datetime
from pprint import pprint

logger = logging.getLogger()
logger.setLevel(logging.INFO)


processed_bucket = "processed-bucket-1158020804995033"


def lambda_handler(event, context):
    # Get secret
    secret_string = get_secret()
    warehouse = json.loads(secret_string)
    user = warehouse["User"]
    host = warehouse["Host"]
    db = warehouse["Schema"]
    port = warehouse["Port"]
    password = warehouse["Password"]
    s3_bucket_name, s3_object_name = get_object_path(event["Records"])
    if s3_object_name == "warehouse_trigger.txt":
        try:
            # delete_tables_in_order(user, host, db, port, password)
            table_utils = {'dim_counterparty': counterparty_util,
                           'dim_currency': currency_util,
                           'dim_design': design_util,
                           'dim_location': location_util,
                           'dim_staff': staff_util,
                           'dim_payment_type': payment_type_util,
                           'dim_transaction': transaction_util,
                           'fact_sales_order': sales_order_util,
                           'fact_purchase_order': purchase_order_util,
                           'fact_payment': payment_util}

            for table_name, util_func in table_utils.items():
                start_time = time.time()
                skipped = util_func(user, host, db, port, password)
                if skipped:
                    end_time = time.time()
                    logger.info(
                        f'{table_name} no updates to data warehouse:\
                        {end_time - start_time} seconds to execute.')
                    print(
                        f'{table_name} no updates to data warehouse:\
                        {end_time - start_time} seconds to execute.')
                else:
                    end_time = time.time()
                    logger.info(
                        f'{table_name} uploaded to the data warehouse:\
                        {end_time - start_time} seconds to execute.')
                    print(
                        f'{table_name} uploaded to the data warehouse:\
                        {end_time - start_time} seconds to execute.')

        except ClientError as e:
            logger.error(f"An S3 has occurred: {e}")
        except Exception as e:
            logger.error(f"An Error has occurred: {e}")


def delete_tables_in_order(user, host, db, port, password):
    tables_in_order = [
        "fact_payment",
        "fact_purchase_order",
        "fact_sales_order",
        "dim_transaction",
        "dim_payment_type",
        "dim_staff",
        "dim_location",
        "dim_design",
        "dim_currency",
        "dim_counterparty"
    ]

    try:
        conn = pg8000.connect(user=user, password=password,
                              host=host, port=port, database=db)
        cursor = conn.cursor()

        for table in tables_in_order:
            cursor.execute(f"DELETE FROM {table}")
            logger.info(f"Deleted data from {table}")
            print(f"Deleted data from {table}")
        conn.commit()
        logger.info("All tables deleted successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"An error occurred while deleting tables: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def get_latest_file_name(prefix):
    bucket_name = "processed-bucket-1158020804995033"
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
        print(f'\n latest filename = {prefix}/{latest_file_name}')
        return latest_file_name
    else:
        return None


def extract_data_from_processed_s3(bucket, key):
    s3_client = boto3.client("s3")
    try:
        res = s3_client.get_object(Bucket=bucket, Key=key)
        parquet_data = io.BytesIO(res["Body"].read())
        df = pd.read_parquet(parquet_data)
        return df
    except ClientError as e:
        logger.error(f"An error occurred while retrieving data from S3: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def get_object_path(records):
    """Extracts bucket and object references from Records field of event."""
    return records[0]["s3"]["bucket"]["name"], records[0]["s3"]["object"]["key"]


def get_secret():
    secret_name = "warehouseSQL"
    region_name = "eu-west-2"
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name)

    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        logger.critical(f"Error getting secret: {e}")
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response["SecretString"]
    return secret


def warehouse_update(table_name, latest):

    s3 = boto3.client('s3')
    try:
        res = s3.get_object(
            Bucket="processed-bucket-1158020804995033", Key='warehouse_update.json')
        data = res['Body'].read().decode('utf-8')
        data = json.loads(data)
        pprint(data)
        if data[table_name] == "null":
            data[table_name] = latest
            skip = False
        else:
            parquet_date = datetime.strptime(latest, '%Y-%m-%d-%H-%M-%S')
            json_date = datetime.strptime(
                data[table_name], '%Y-%m-%d-%H-%M-%S')
            if parquet_date > json_date:
                data[table_name] = parquet_date.strftime('%Y-%m-%d-%H-%M-%S')
                skip = False
            else:
                skip = True
        parquet_file_name = data[table_name]
        data = json.dumps(data)
        s3.put_object(Bucket="processed-bucket-1158020804995033",
                      Key='warehouse_update.json', Body=data)
        return parquet_file_name, skip
    except ClientError as e:
        logger.error(
            f"An error occurred while retrieving warehouse_update.json from S3: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e


def counterparty_util(user, host, db, port, password):
    latest = get_latest_file_name('dim_counterparty')
    latest_parquet, skip = warehouse_update('dim_counterparty', latest)
    if skip:
        return True
    else:
        key = f'dim_counterparty/{latest_parquet}.parquet'
        counterparty_data = extract_data_from_processed_s3(
            processed_bucket, key
        )
        try:
            conn = pg8000.connect(user=user, password=password,
                                  host=host, port=port, database=db)
            cursor = conn.cursor()
            try:
                counterparty_data = counterparty_data.fillna('Null')
                for index, row in counterparty_data.iterrows():
                    cursor.execute('''SELECT * FROM dim_counterparty WHERE counterparty_id = %s''', (row['counterparty_id'],))
                    result = cursor.fetchone()
                    if result:
                        cursor.execute('''UPDATE dim_counterparty SET counterparty_legal_name = %s,
                                        counterparty_legal_address_line_1 = %s,
                                        counterparty_legal_address_line_2 = %s,
                                        counterparty_legal_district = %s,
                                        counterparty_legal_city = %s,
                                        counterparty_legal_postal_code = %s,
                                        counterparty_legal_country = %s,
                                        counterparty_legal_phone_number = %s
                                        WHERE counterparty_id = %s''',
                                    (
                                        row['counterparty_legal_name'],
                                        row['counterparty_legal_address_line_1'],
                                        row['counterparty_legal_address_line_2'],
                                        row['counterparty_legal_district'],
                                        row['counterparty_legal_city'],
                                        row['counterparty_legal_postal_code'],
                                        row['counterparty_legal_country'],
                                        row['counterparty_legal_phone_number'],
                                        row['counterparty_id']
                                    )
                                    )
                    else:
                        cursor.execute('''INSERT INTO dim_counterparty (counterparty_id,
                                        counterparty_legal_name,
                                        counterparty_legal_address_line_1,
                                        counterparty_legal_address_line_2,
                                        counterparty_legal_district,
                                        counterparty_legal_city,
                                        counterparty_legal_postal_code,
                                        counterparty_legal_country,
                                        counterparty_legal_phone_number)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                                    (
                                        row['counterparty_id'],
                                        row['counterparty_legal_name'],
                                        row['counterparty_legal_address_line_1'],
                                        row['counterparty_legal_address_line_2'],
                                        row['counterparty_legal_district'],
                                        row['counterparty_legal_city'],
                                        row['counterparty_legal_postal_code'],
                                        row['counterparty_legal_country'],
                                        row['counterparty_legal_phone_number']
                                    )
                                    )
                conn.commit()

                pass
            except Exception as e:
                conn.rollback()
                logger.error(
                    f"An error occurred while executing SQL queries: {e}")
        except Exception as e:
            logger.error(
                f"An error occurred while connecting to the database: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
            return False



def currency_util(user, host, db, port, password):
    latest = get_latest_file_name('dim_currency')
    latest_parquet, skip = warehouse_update('dim_currency', latest)

    if skip:
        return True
    else:
        key = f'dim_currency/{latest_parquet}.parquet'
        currency_data = extract_data_from_processed_s3(
            processed_bucket, key)
        try:
            conn = pg8000.connect(user=user, password=password,
                                  host=host, port=port, database=db)
            cursor = conn.cursor()
            try:
                for index, row in currency_data.iterrows():
                    cursor.execute('''SELECT * FROM dim_currency WHERE currency_id = %s''', (row['currency_id'],))
                    result = cursor.fetchone()
                    if result:
                        cursor.execute('''UPDATE dim_currency SET currency_code = %s,
                                        currency_name = %s
                                        WHERE currency_id = %s''',
                                       (
                                           row['currency_code'],
                                           row['currency_name'],
                                           row['currency_id']
                                       )
                                       )
                    else:
                        cursor.execute('''INSERT INTO dim_currency
                                    (currency_id,currency_code ,currency_name )
                                    VALUES (%s, %s, %s)''',
                                       (row['currency_id'],
                                        row['currency_code'],
                                        row['currency_name']))
                conn.commit()
            except Exception as e:
                logger.error(
                    f"An error occurred while executing SQL queries: {e}")
        except Exception as e:
            conn.rollback()
            logger.error(
                f"An error occurred while connecting to the database: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
            return False


def date_util(user, host, db, port, password):
    date_data = extract_data_from_processed_s3(
        processed_bucket, "dim_date.parquet"
    )
    try:
        conn = pg8000.connect(user=user, password=password,
                              host=host, port=port, database=db)
        cursor = conn.cursor()
        try:
            date_data.index = pd.RangeIndex(
                start=1, stop=len(date_data) + 1, step=1)
            date_data['quarter'] = date_data['date_id'].dt.quarter
            for index, row in date_data.iterrows():
                cursor.execute('''INSERT INTO dim_date (date_id,
                                year,
                                month,
                                day,
                                day_of_week,
                                day_name,
                                month_name,
                                quarter)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''',
                               (
                                   row['date_id'],
                                   row['year'],
                                   row['month'],
                                   row['day'],
                                   row['day_of_week'],
                                   row['day_name'],
                                   row['month_name'],
                                   row['quarter']
                               )
                               )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"An error occurred while executing SQL queries: {e}")
    except Exception as e:
        logger.error(
            f"An error occurred while connecting to the database: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()



def design_util(user, host, db, port, password):
    latest = get_latest_file_name('dim_design')
    latest_parquet, skip = warehouse_update('dim_design', latest)
    if skip:
        return True
    else:
        key = f'dim_design/{latest_parquet}.parquet'
        design_data = extract_data_from_processed_s3(
            processed_bucket, key
        )
        try:
            conn = pg8000.connect(user=user, password=password,
                                  host=host, port=port, database=db)
            cursor = conn.cursor()
            try:
                for index, row in design_data.iterrows():
                    cursor.execute('''SELECT * FROM dim_design WHERE design_id = %s''', (row['design_id'],))
                    result = cursor.fetchone()
                    if result:
                        cursor.execute('''UPDATE dim_design SET design_name = %s,
                                        file_location = %s,
                                        file_name = %s
                                        WHERE design_id = %s''',
                                       (
                                           row['design_name'],
                                           row['file_location'],
                                           row['file_name'],
                                           row['design_id']
                                       )
                                       )
                    else:
                        cursor.execute('''INSERT INTO dim_design (design_id,
                                    design_name,
                                    file_location,
                                    file_name)
                                    VALUES (%s, %s, %s, %s)''',
                                       (
                                           row['design_id'],
                                           row['design_name'],
                                           row['file_location'],
                                           row['file_name']
                                       )
                                       )
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(
                    f"An error occurred while executing SQL queries: {e}")
        except Exception as e:
            logger.error(
                f"An error occurred while connecting to the database: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
            return False



def location_util(user, host, db, port, password):
    latest = get_latest_file_name('dim_location')
    latest_parquet, skip = warehouse_update('dim_location', latest)
    if skip:
        return True
    else:
        key = f'dim_location/{latest_parquet}.parquet'
        location_data = extract_data_from_processed_s3(
            processed_bucket, key
        )
        try:
            conn = pg8000.connect(user=user, password=password,
                                  host=host, port=port, database=db)
            cursor = conn.cursor()
            try:
                for index, row in location_data.iterrows():
                    cursor.execute('''SELECT * FROM dim_location WHERE location_id = %s''', (row['location_id'],))
                    result = cursor.fetchone()
                    if result:
                        cursor.execute('''UPDATE dim_location SET address_line_1 = %s,
                                        address_line_2 = %s,
                                        district = %s,
                                        city = %s,
                                        postal_code = %s,
                                        country = %s,
                                        phone = %s
                                        WHERE location_id = %s''',
                                       (
                                           row['address_line_1'],
                                           row['address_line_2'],
                                           row['district'],
                                           row['city'],
                                           row['postal_code'],
                                           row['country'],
                                           row['phone'],
                                           row['location_id']
                                       )
                                       )
                    else:
                        cursor.execute('''INSERT INTO dim_location (location_id,
                                    address_line_1,
                                    address_line_2,
                                    district,
                                    city,
                                    postal_code,
                                    country,
                                    phone)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''',
                                       (
                                           row['location_id'],
                                           row['address_line_1'],
                                           row['address_line_2'],
                                           row['district'],
                                           row['city'],
                                           row['postal_code'],
                                           row['country'],
                                           row['phone']
                                       )
                                       )
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(
                    f"An error occurred while executing SQL queries: {e}")
        except Exception as e:
            logger.error(
                f"An error occurred while connecting to the database: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
            return False



def staff_util(user, host, db, port, password):
    latest = get_latest_file_name('dim_staff')
    latest_parquet, skip = warehouse_update('dim_staff', latest)
    if skip:
        return True
    else:
        key = f'dim_staff/{latest_parquet}.parquet'
        staff_data = extract_data_from_processed_s3(
            processed_bucket, key
        )
        try:
            conn = pg8000.connect(user=user, password=password,
                                  host=host, port=port, database=db)
            cursor = conn.cursor()
            try:
                for index, row in staff_data.iterrows():
                    cursor.execute('''SELECT * FROM dim_staff WHERE staff_id = %s''', (row['staff_id'],))
                    result = cursor.fetchone()
                    if result:
                        cursor.execute('''UPDATE dim_staff SET first_name = %s,
                                        last_name = %s,
                                        email_address = %s,
                                        department_name = %s,
                                        location = %s
                                        WHERE staff_id = %s''',
                                       (
                                           row['first_name'],
                                           row['last_name'],
                                           row['email_address'],
                                           row['department_name'],
                                           row['location'],
                                           row['staff_id']
                                       )
                                       )
                    else:
                        cursor.execute('''INSERT INTO dim_staff (staff_id,
                                    first_name,
                                    last_name,
                                    email_address,
                                    department_name,
                                    location)
                                    VALUES (%s, %s, %s, %s, %s, %s)''',
                                       (
                                           row['staff_id'],
                                           row['first_name'],
                                           row['last_name'],
                                           row['email_address'],
                                           row['department_name'],
                                           row['location']
                                       )
                                       )
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(
                    f"An error occurred while executing SQL queries: {e}")
        except Exception as e:
            logger.error(
                f"An error occurred while connecting to the database: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
            return False




def payment_type_util(user, host, db, port, password):
    latest = get_latest_file_name('dim_payment_type')
    latest_parquet, skip = warehouse_update('dim_payment_type', latest)
    if skip:
        return True
    else:
        key = f'dim_payment_type/{latest_parquet}.parquet'
        payment_type_data = extract_data_from_processed_s3(
            processed_bucket, key
        )
        try:
            conn = pg8000.connect(user=user, password=password,
                                  host=host, port=port, database=db)
            cursor = conn.cursor()
            try:
                for index, row in payment_type_data.iterrows():
                    cursor.execute('''SELECT * FROM dim_payment_type WHERE payment_type_id = %s''', (row['payment_type_id'],))
                    result = cursor.fetchone()
                    if result:
                        cursor.execute('''UPDATE dim_payment_type SET payment_type_name = %s
                                        WHERE payment_type_id = %s''',
                                       (
                                           row['payment_type_name'],
                                           row['payment_type_id']
                                       )
                                       )
                    else:
                        cursor.execute('''INSERT INTO dim_payment_type
                                    (payment_type_id, payment_type_name )
                                    VALUES (%s, %s)''',
                                       (row['payment_type_id'],
                                        row['payment_type_name']))
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(
                    f"An error occurred while executing SQL queries: {e}")
        except Exception as e:
            logger.error(
                f"An error occurred while connecting to the database: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
            return False




def transaction_util(user, host, db, port, password):
    latest = get_latest_file_name('dim_transaction')
    latest_parquet, skip = warehouse_update('dim_transaction', latest)
    if skip:
        return True
    else:
        key = f'dim_transaction/{latest_parquet}.parquet'
        transaction_data = extract_data_from_processed_s3(
            processed_bucket, key
        )
        try:
            conn = pg8000.connect(user=user, password=password,
                                  host=host, port=port, database=db)
            cursor = conn.cursor()
            try:
                for index, row in transaction_data.iterrows():
                    cursor.execute('''SELECT * FROM dim_transaction WHERE transaction_id = %s''', (row['transaction_id'],))
                    result = cursor.fetchone()
                    if result:
                        cursor.execute('''UPDATE dim_transaction SET transaction_type = %s,
                                        sales_order_id = %s,
                                        purchase_order_id = %s
                                        WHERE transaction_id = %s''',
                                       (
                                           row['transaction_type'],
                                           row['sales_order_id'],
                                           row['purchase_order_id'],
                                           row['transaction_id']
                                       )
                                       )
                    else:
                        cursor.execute('''INSERT INTO dim_transaction (transaction_id,
                                    transaction_type,
                                    sales_order_id,
                                    purchase_order_id)
                                    VALUES (%s, %s, %s, %s)''',
                                       (
                                           row['transaction_id'],
                                           row['transaction_type'],
                                           row['sales_order_id'],
                                           row['purchase_order_id']
                                       )
                                       )
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(
                    f"An error occurred while executing SQL queries: {e}")
        except Exception as e:
            logger.error(
                f"An error occurred while connecting to the database: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
            return False




def sales_order_util(user, host, db, port, password):
    latest = get_latest_file_name('fact_sales_order')
    latest_parquet, skip = warehouse_update('fact_sales_order', latest)
    if skip:
        return True
    else:
        key = f'fact_sales_order/{latest_parquet}.parquet'
        sales_order_data = extract_data_from_processed_s3(
            processed_bucket, key
        )
        try:
            conn = pg8000.connect(user=user, password=password,
                                  host=host, port=port, database=db)
            cursor = conn.cursor()
            try:
                sales_order_data = sales_order_data.rename(
                    columns={'staff_id': 'sales_staff_id'})
                for index, row in sales_order_data.iterrows():
                    cursor.execute('''SELECT * FROM fact_sales_order WHERE sales_order_id = %s''', (row['sales_order_id'],))
                    result = cursor.fetchone()
                    if result:
                        cursor.execute('''UPDATE fact_sales_order SET created_date = %s,
                                        created_time = %s,
                                        last_updated_date = %s,
                                        last_updated_time = %s,
                                        sales_staff_id = %s,
                                        counterparty_id = %s,
                                        units_sold = %s,
                                        unit_price = %s,
                                        currency_id = %s,
                                        design_id = %s,
                                        agreed_payment_date = %s,
                                        agreed_delivery_date = %s,
                                        agreed_delivery_location_id = %s
                                        WHERE sales_order_id = %s''',
                                       (
                                           row['created_date'],
                                           row['created_time'],
                                           row['last_updated_date'],
                                           row['last_updated_time'],
                                           row['sales_staff_id'],
                                           row['counterparty_id'],
                                           row['units_sold'],
                                           row['unit_price'],
                                           row['currency_id'],
                                           row['design_id'],
                                           row['agreed_payment_date'],
                                           row['agreed_delivery_date'],
                                           row['counterparty_id'],
                                           row['sales_order_id']
                                       )
                                       )
                    else:
                        cursor.execute('''INSERT INTO fact_sales_order (sales_order_id,
                                    created_date,
                                    created_time,
                                    last_updated_date,
                                    last_updated_time,
                                    sales_staff_id,
                                    counterparty_id,
                                    units_sold,
                                    unit_price,
                                    currency_id,
                                    design_id,
                                    agreed_payment_date,
                                    agreed_delivery_date,
                                    agreed_delivery_location_id)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s)''',
                                       (
                                           row['sales_order_id'],
                                           row['created_date'],
                                           row['created_time'],
                                           row['last_updated_date'],
                                           row['last_updated_time'],
                                           row['sales_staff_id'],
                                           row['counterparty_id'],
                                           row['units_sold'],
                                           row['unit_price'],
                                           row['currency_id'],
                                           row['design_id'],
                                           row['agreed_payment_date'],
                                           row['agreed_delivery_date'],
                                           row['counterparty_id']
                                       )
                                       )
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(
                    f"An error occurred while executing SQL queries: {e}")
        except Exception as e:
            logger.error(
                f"An error occurred while connecting to the database: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
            return False




def purchase_order_util(user, host, db, port, password):
    latest = get_latest_file_name('fact_purchase_order')
    latest_parquet, skip = warehouse_update('fact_purchase_order', latest)
    if skip:
        return True
    else:
        key = f'fact_purchase_order/{latest_parquet}.parquet'
        purchase_order_data = extract_data_from_processed_s3(
            processed_bucket, key
        )
        try:
            conn = pg8000.connect(user=user, password=password,
                                  host=host, port=port, database=db)
            cursor = conn.cursor()
            try:
                for index, row in purchase_order_data.iterrows():
                    cursor.execute('''SELECT * FROM fact_purchase_order WHERE purchase_order_id = %s''', (row['purchase_order_id'],))
                    result = cursor.fetchone()
                    if result:
                        cursor.execute('''UPDATE fact_purchase_order SET created_date = %s,
                                        created_time = %s,
                                        last_updated_date = %s,
                                        last_updated_time = %s,
                                        staff_id = %s,
                                        counterparty_id = %s,
                                        item_code = %s,
                                        item_quantity = %s,
                                        item_unit_price = %s,
                                        currency_id = %s,
                                        agreed_delivery_date = %s,
                                        agreed_payment_date = %s,
                                        agreed_delivery_location_id = %s
                                        WHERE purchase_order_id = %s''',
                                       (
                                           row['created_date'],
                                           row['created_time'],
                                           row['last_updated_date'],
                                           row['last_updated_time'],
                                           row['staff_id'],
                                           row['counterparty_id'],
                                           row['item_code'],
                                           row['item_quantity'],
                                           row['item_unit_price'],
                                           row['currency_id'],
                                           row['agreed_delivery_date'],
                                           row['agreed_payment_date'],
                                           row['agreed_delivery_location_id'],
                                           row['purchase_order_id']
                                       )
                                       )
                    else:
                        cursor.execute('''INSERT INTO fact_purchase_order (purchase_order_id,
                                    created_date,
                                    created_time,
                                    last_updated_date,
                                    last_updated_time,
                                    staff_id,
                                    counterparty_id,
                                    item_code,
                                    item_quantity,
                                    item_unit_price,
                                    currency_id,
                                    agreed_delivery_date,
                                    agreed_payment_date,
                                    agreed_delivery_location_id)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s)''',
                                       (
                                           row['purchase_order_id'],
                                           row['created_date'],
                                           row['created_time'],
                                           row['last_updated_date'],
                                           row['last_updated_time'],
                                           row['staff_id'],
                                           row['counterparty_id'],
                                           row['item_code'],
                                           row['item_quantity'],
                                           row['item_unit_price'],
                                           row['currency_id'],
                                           row['agreed_delivery_date'],
                                           row['agreed_payment_date'],
                                           row['agreed_delivery_location_id']
                                       )
                                       )
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(
                    f"An error occurred while executing SQL queries: {e}")
        except Exception as e:
            logger.error(
                f"An error occurred while connecting to the database: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
            return False




def payment_util(user, host, db, port, password):
    latest = get_latest_file_name('fact_payment')
    latest_parquet, skip = warehouse_update('fact_payment', latest)
    if skip:
        return True
    else:
        key = f'fact_payment/{latest_parquet}.parquet'
        payment_data = extract_data_from_processed_s3(
            processed_bucket, key
        )
        try:
            conn = pg8000.connect(user=user, password=password,
                                  host=host, port=port, database=db)
            cursor = conn.cursor()
            try:
                for index, row in payment_data.iterrows():
                    cursor.execute('''SELECT * FROM fact_payment WHERE payment_id = %s''', (row['payment_id'],))
                    result = cursor.fetchone()
                    if result:
                        cursor.execute('''UPDATE fact_payment SET created_date = %s,
                                        created_time = %s,
                                        last_updated_date = %s,
                                        last_updated_time = %s,
                                        transaction_id = %s,
                                        counterparty_id = %s,
                                        payment_amount = %s,
                                        currency_id = %s,
                                        payment_type_id = %s,
                                        paid = %s,
                                        payment_date = %s
                                        WHERE payment_id = %s''',
                                       (
                                           row['created_date'],
                                           row['created_time'],
                                           row['last_updated_date'],
                                           row['last_updated_time'],
                                           row['transaction_id'],
                                           row['counterparty_id'],
                                           row['payment_amount'],
                                           row['currency_id'],
                                           row['payment_type_id'],
                                           row['paid'],
                                           row['payment_date'],
                                           row['payment_id']
                                       )
                                       )
                    else:
                        cursor.execute('''INSERT INTO fact_payment (payment_id,
                                    created_date,
                                    created_time,
                                    last_updated_date,
                                    last_updated_time,
                                    transaction_id,
                                    counterparty_id,
                                    payment_amount,
                                    currency_id,
                                    payment_type_id,
                                    paid,
                                    payment_date
                                    )
                                    VALUES (%s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s)''',
                                       (
                                           row['payment_id'],
                                           row['created_date'],
                                           row['created_time'],
                                           row['last_updated_date'],
                                           row['last_updated_time'],
                                           row['transaction_id'],
                                           row['counterparty_id'],
                                           row['payment_amount'],
                                           row['currency_id'],
                                           row['payment_type_id'],
                                           row['paid'],
                                           row['payment_date']
                                       )
                                       )
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(
                    f"An error occurred while executing SQL queries: {e}")
        except Exception as e:
            logger.error(
                f"An error occurred while connecting to the database: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
            return False

