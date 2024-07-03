import logging
import boto3
import pg8000.native
from botocore.exceptions import ClientError
import json
import time
from datetime import datetime, timedelta

ingestion_bucket = "ingestion-bucket-1158020804995033"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    # Get secret
    secret_string = get_secret()
    tote = json.loads(secret_string)
    user = tote["PGUSER"]
    host = tote["PGURL"]
    db = tote["PGDATABASE"]
    port = tote["PGPORT"]
    password = tote["PGPASSWORD"]

    s3 = boto3.client("s3")

    try:
        file_func = {'counterparty': get_counterparty_data,
                     'address': get_address_data,
                     'currency': get_currency_util,
                     'department': get_department_data,
                     'design': get_design_data,
                     'payment': get_payment_data,
                     'payment_type': get_payment_type_data,
                     'purchase_order': get_purchase_order_data,
                     'sales_order': get_sales_order_util,
                     'staff': get_staff_data,
                     'transaction': get_transaction_data}

        for file_name, data_func in file_func.items():
            start_time = time.time()
            last_updated = get_latest_file_date(ingestion_bucket, file_name)
            data, max_last_updated = data_func(
                user, host, db, port, password, last_updated)
            if data['data']:
                date_string = max_last_updated.strftime('%Y-%m-%d-%H-%M-%S')
                filename = f"{file_name}/{date_string}.json"

                json_data = json.dumps(data)
                s3.put_object(Body=json_data,
                              Bucket=ingestion_bucket, Key=filename)

                end_time = time.time()
                logger.info(f'{file_name} data saved to S3:\
                        {end_time - start_time} seconds to execute.')
            else:
                end_time = time.time()
                logger.info(f'{file_name} no new data in database:\
                      {end_time - start_time} seconds to execute.')
            if file_name == 'transaction':
                trigger_lambda_processed()
    except ClientError as e:
        logger.error(f'An S3 Error occurred: {e}')
        raise e
    except Exception as e:
        logger.error(f'An Error occurred: {e}')
        raise e


def get_secret():

    secret_name = "totesql"
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
        logger.critical(f"Error getting totesql secret: {e}")
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response["SecretString"]
    return secret


def trigger_lambda_processed():
    s3 = boto3.client('s3')
    try:
        res = s3.get_object(
            Bucket="ingestion-bucket-1158020804995033", Key='processed_trigger.txt')
        data = res['Body'].read().decode('utf-8')
        s3.put_object(Bucket="ingestion-bucket-1158020804995033",
                      Key='processed_trigger.txt', Body=data)
        print('\nlambda 2 trigger........\n')
    except ClientError as e:
        logger.error(
            f"An error occurred while retrieving processed_trigger.txt from S3: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e


def get_latest_file_date(bucket_name, prefix):

    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    dates = []
    for obj in response.get('Contents', []):
        key = obj['Key']
        date_string = key.split('/')[-1].split('.')[0]
        date = datetime.strptime(date_string, '%Y-%m-%d-%H-%M-%S')
        dates.append(date)

    if dates:
        latest_date = max(dates)
        return latest_date
    else:
        return None


def format_to_dict(rows, column_titles):
    """Formats rows of data into a dictionary.

    This function takes in rows of data and their corresponding
    column titles and formats them into a dictionary.
    The resulting dictionary has keys that correspond to the column
    titles and values that correspond to the data in the rows.
    The function also converts certain values to strings and
    formats datetime objects.

    Args:
        rows (list): A list of rows of data.
        column_titles (list): A list of column titles.

    Returns:
        dict: A dictionary containing the formatted data.
    """
    try:
        data = []

        for element in rows:
            row = {}
            i = 0
            for name in column_titles:
                row[name] = element[i]
                i += 1

            data.append(row)

        for element in data:
            if 'unit_price' in element:
                element['unit_price'] = str(element['unit_price'])
            elif 'payment_amount' in element:
                element['payment_amount'] = str(element['payment_amount'])
            elif 'item_unit_price' in element:
                element['item_unit_price'] = str(element['item_unit_price'])
            time1 = element['created_at']
            element['created_at'] = time1.strftime("%Y-%m-%d, %X:%f")
            time2 = element['last_updated']
            element['last_updated'] = time2.strftime("%Y-%m-%d, %X:%f")

        return {'data': data}
    except Exception as e:
        logger.error(f"format_to_dict: An error occurred: {e}")


def get_counterparty_data(user, host, db, port, password, last_updated=None):
    """Retrieves counterparty data from a database.

    This function connects to a database using the provided
    parameters and retrieves counterparty data from the
    'counterparty' table. The data is then formatted into
    a dictionary and returned.


    Args:
        user (str): The username for the database connection.
        host (str): The hostname for the database connection.
        db (str): The name of the database to connect to.
        port (int): The port number for the database connection.
        password (str): The password for the database connection.
        last_updated (str): The minimum value for the last_updated column.

    Returns:

        tuple: A tuple containing a dictionary with the counterparty data and a string with the maximum value of the last_updated column.
    """
    # conn = None
    try:
        conn = pg8000.native.Connection(
            user, host=host, database=db, port=port, password=password)

        try:
            if last_updated is None:
                data_query = '''SELECT * FROM counterparty ORDER BY last_updated;'''
            else:
                data_query = f'''SELECT * FROM counterparty WHERE last_updated > '{last_updated}' ORDER BY last_updated;'''

            rows = conn.run(data_query)
            columns = conn.columns
            column_titles = [c['name'] for c in columns]
            counterparty_data = format_to_dict(rows, column_titles)

            max_query = '''SELECT MAX(last_updated) FROM counterparty;'''
            max_last_updated = conn.run(max_query)[0][0]
            max_last_updated += timedelta(seconds=1)
            max_last_updated = max_last_updated.replace(microsecond=0)

            return counterparty_data, max_last_updated
        except Exception as e:
            logger.error(
                f'An error occurred while executing the SQL query: {e}')
    except Exception as e:
        logger.error(
            f'An error occurred while connecting to the database: {e}')
    finally:
        conn.close()


def get_address_data(user, host, db, port, password, last_updated=None):
    """Retrieves address data from a database.

    This function connects to a database using the provided parameters
    and retrieves address data from the 'address' table. The data is
    then formatted into a dictionary and returned.

    Args:
        user (str): The username for the database connection.
        host (str): The hostname for the database connection.
        db (str): The name of the database to connect to.
        port (int): The port number for the database connection.
        password (str): The password for the database connection.
        last_updated (str): The minimum value for the last_updated column.

    Returns:
        tuple: A tuple containing a dictionary with the address data and a string with the maximum value of the last_updated column.
    """
    try:
        conn = pg8000.native.Connection(
            user, host=host, database=db, port=port, password=password)

        try:
            if last_updated is None:
                data_query = '''SELECT * FROM address ORDER BY last_updated;'''
            else:
                data_query = f'''SELECT * FROM address WHERE last_updated > '{last_updated}' ORDER BY last_updated;'''

            rows = conn.run(data_query)
            columns = conn.columns
            column_titles = [c['name'] for c in columns]
            address_data = format_to_dict(rows, column_titles)

            max_query = '''SELECT MAX(last_updated) FROM address;'''
            max_last_updated = conn.run(max_query)[0][0]
            max_last_updated += timedelta(seconds=1)
            max_last_updated = max_last_updated.replace(microsecond=0)

            return address_data, max_last_updated
        except Exception as e:
            logger.error(
                f'An error occurred while executing the SQL query: {e}')
    except Exception as e:
        logger.error(
            f'An error occurred while connecting to the database: {e}')
    finally:
        conn.close()


def get_currency_util(user, host, db, port, password, last_updated=None):
    """Retrieves currency data from a database.

    This function connects to a database using the provided
    parameters and retrieves currency data from the 'currency' table.
    The data is then formatted into a dictionary and returned.

    Args:
        user (str): The username for the database connection.
        host (str): The hostname for the database connection.
        db (str): The name of the database to connect to.
        port (int): The port number for the database connection.
        password (str): The password for the database connection.
        last_updated (str): The minimum value for the last_updated column.

    Returns:
        tuple: A tuple containing a dictionary with the currency data and a string with the maximum value of the last_updated column.
    """
    try:
        conn = pg8000.native.Connection(
            user, host=host, database=db, port=port, password=password
        )

        try:
            if last_updated is None:
                data_query = '''SELECT * FROM currency ORDER BY last_updated;'''
            else:
                data_query = f'''SELECT * FROM currency WHERE last_updated > '{last_updated}' ORDER BY last_updated;'''

            rows = conn.run(data_query)
            columns = conn.columns
            column_titles = [c["name"] for c in columns]
            currency_data = format_to_dict(rows, column_titles)
            max_query = '''SELECT MAX(last_updated) FROM currency;'''
            max_last_updated = conn.run(max_query)[0][0]
            max_last_updated += timedelta(seconds=1)
            max_last_updated = max_last_updated.replace(microsecond=0)
            return currency_data, max_last_updated
        except Exception as e:
            logger.error(
                f'An error occurred while executing the SQL query: {e}')
    except Exception as e:
        logger.error(
            f'An error occurred while connecting to the database: {e}')
    finally:
        conn.close()


def get_department_data(user, host, db, port, password, last_updated=None):
    """Retrieves department data from a database.

    This function connects to a database using the provided
    parameters and retrieves department data from the 'department' table.
    The data is then formatted into a dictionary and returned.

    Args:
        user (str): The username for the database connection.
        host (str): The hostname for the database connection.
        db (str): The name of the database to connect to.
        port (int): The port number for the database connection.
        password (str): The password for the database connection.
        last_updated (str): The minimum value for the last_updated column.

    Returns:
        tuple: A tuple containing a dictionary with the department data and a string with the maximum value of the last_updated column.
    """
    try:
        conn = pg8000.native.Connection(
            user, host=host, database=db, port=port, password=password)

        try:
            if last_updated is None:
                data_query = '''SELECT * FROM department ORDER BY last_updated;'''
            else:
                data_query = f'''SELECT * FROM department WHERE last_updated > '{last_updated}' ORDER BY last_updated;'''

            rows = conn.run(data_query)
            columns = conn.columns
            column_titles = [c['name'] for c in columns]
            department_data = format_to_dict(rows, column_titles)

            max_query = '''SELECT MAX(last_updated) FROM department;'''
            max_last_updated = conn.run(max_query)[0][0]
            max_last_updated += timedelta(seconds=1)
            max_last_updated = max_last_updated.replace(microsecond=0)

            return department_data, max_last_updated
        except Exception as e:
            logger.error(
                f'An error occurred while executing the SQL query: {e}')
    except Exception as e:
        logger.error(
            f'An error occurred while connecting to the database: {e}')
    finally:
        conn.close()


def get_design_data(user, host, db, port, password, last_updated=None):
    """Retrieves design data from a database.

    This function connects to a database using the provided
    parameters and retrieves design data from the 'design' table.
    The data is then formatted into a dictionary and returned.

    Args:
        user (str): The username for the database connection.
        host (str): The hostname for the database connection.
        db (str): The name of the database to connect to.
        port (int): The port number for the database connection.
        password (str): The password for the database connection.
        last_updated (str): The minimum value for the last_updated column.

    Returns:
        tuple: A tuple containing a dictionary with the design data and a string with the maximum value of the last_updated column.
    """
    try:
        conn = pg8000.native.Connection(
            user, host=host, database=db, port=port, password=password)
        try:
            if last_updated is None:
                data_query = '''SELECT * FROM design ORDER BY last_updated;'''
            else:
                data_query = f'''SELECT * FROM design WHERE last_updated > '{last_updated}' ORDER BY last_updated;'''

            rows = conn.run(data_query)
            columns = conn.columns
            column_titles = [c['name'] for c in columns]
            design_data = format_to_dict(rows, column_titles)

            max_query = '''SELECT MAX(last_updated) FROM design;'''
            max_last_updated = conn.run(max_query)[0][0]
            max_last_updated += timedelta(seconds=1)
            max_last_updated = max_last_updated.replace(microsecond=0)

            return design_data, max_last_updated
        except Exception as e:
            logger.error(
                f'An error occurred while executing the SQL query: {e}')
    except Exception as e:
        logger.error(
            f'An error occurred while connecting to the database: {e}')
    finally:
        conn.close()


def get_payment_data(user, host, db, port, password, last_updated=None):
    """Retrieves payment data from a database.

    This function connects to a database using the provided
    parameters and retrieves payment data from the 'payment'
    table. The data is then formatted into a dictionary and returned.

    Args:
        user (str): The username for the database connection.
        host (str): The hostname for the database connection.
        db (str): The name of the database to connect to.
        port (int): The port number for the database connection.
        password (str): The password for the database connection.
        last_updated (str): The minimum value for the last_updated column.

    Returns:
        tuple: A tuple containing a dictionary with the payment data and a string with the maximum value of the last_updated column.
    """
    try:
        conn = pg8000.native.Connection(
            user, host=host, database=db, port=port, password=password)
        try:
            if last_updated is None:
                data_query = '''SELECT * FROM payment ORDER BY last_updated;'''
            else:
                data_query = f'''SELECT * FROM payment WHERE last_updated > '{last_updated}' ORDER BY last_updated;'''

            rows = conn.run(data_query)
            columns = conn.columns
            column_titles = [c['name'] for c in columns]
            payment_data = format_to_dict(rows, column_titles)

            max_query = '''SELECT MAX(last_updated) FROM payment;'''
            max_last_updated = conn.run(max_query)[0][0]
            max_last_updated += timedelta(seconds=1)
            max_last_updated = max_last_updated.replace(microsecond=0)

            return payment_data, max_last_updated
        except Exception as e:
            logger.error(
                f'An error occurred while executing the SQL query: {e}')
    except Exception as e:
        logger.error(
            f'An error occurred while connecting to the database: {e}')
    finally:
        conn.close()


def get_payment_type_data(user, host, db, port, password, last_updated=None):
    """Retrieves payment type data from a database.

    This function connects to a database using the provided parameters
    and retrieves payment type data from the 'payment_type' table.
    The data is then formatted into a dictionary and returned.

    Args:
        user (str): The username for the database connection.
        host (str): The hostname for the database connection.
        db (str): The name of the database to connect to.
        port (int): The port number for the database connection.
        password (str): The password for the database connection.
        last_updated (str): The minimum value for the last_updated column.

    Returns:
        tuple: A tuple containing a dictionary with the payment type data and a string with the maximum value of the last_updated column.
    """
    try:
        conn = pg8000.native.Connection(
            user, host=host, database=db, port=port, password=password)
        try:
            if last_updated is None:
                data_query = '''SELECT * FROM payment_type ORDER BY last_updated;'''
            else:
                data_query = f'''SELECT * FROM payment_type WHERE last_updated > '{last_updated}' ORDER BY last_updated;'''

            rows = conn.run(data_query)
            columns = conn.columns
            column_titles = [c['name'] for c in columns]
            payment_type_data = format_to_dict(rows, column_titles)

            max_query = '''SELECT MAX(last_updated) FROM payment_type;'''
            max_last_updated = conn.run(max_query)[0][0]
            max_last_updated += timedelta(seconds=1)
            max_last_updated = max_last_updated.replace(microsecond=0)

            return payment_type_data, max_last_updated
        except Exception as e:
            logger.error(
                f'An error occurred while executing the SQL query: {e}')
    except Exception as e:
        logger.error(
            f'An error occurred while connecting to the database: {e}')
    finally:
        conn.close()


def get_purchase_order_data(user, host, db, port, password, last_updated=None):
    """Retrieves purchase order data from a database.

    This function connects to a database using the provided
    parameters and retrieves purchase order data from the
    'purchase_order' table. The data is then formatted into
    a dictionary and returned.

    Args:
        user (str): The username for the database connection.
        host (str): The hostname for the database connection.
        db (str): The name of the database to connect to.
        port (int): The port number for the database connection.
        password (str): The password for the database connection.
        last_updated (str): The minimum value for the last_updated column.

    Returns:
        tuple: A tuple containing a dictionary with the purchase order data and a string with the maximum value of the last_updated column.
    """
    try:
        conn = pg8000.native.Connection(
            user, host=host, database=db, port=port, password=password)
        try:
            if last_updated is None:
                data_query = '''SELECT * FROM purchase_order ORDER BY last_updated;'''
            else:
                data_query = f'''SELECT * FROM purchase_order WHERE last_updated > '{last_updated}' ORDER BY last_updated;'''

            rows = conn.run(data_query)
            columns = conn.columns
            column_titles = [c['name'] for c in columns]
            purchase_order_data = format_to_dict(rows, column_titles)

            max_query = '''SELECT MAX(last_updated) FROM purchase_order;'''
            max_last_updated = conn.run(max_query)[0][0]
            max_last_updated += timedelta(seconds=1)
            max_last_updated = max_last_updated.replace(microsecond=0)

            return purchase_order_data, max_last_updated
        except Exception as e:
            logger.error(
                f'An error occurred while executing the SQL query: {e}')
    except Exception as e:
        logger.error(
            f'An error occurred while connecting to the database: {e}')
    finally:
        conn.close()


def get_sales_order_util(user, host, db, port, password, last_updated=None):
    """Retrieves sales order data from a database.

    This function connects to a database using the provided
    parameters and retrieves sales order data from the 'sales_order'
    table. The data is then formatted into a dictionary and returned.

    Args:
        user (str): The username for the database connection.
        host (str): The hostname for the database connection.
        db (str): The name of the database to connect to.
        port (int): The port number for the database connection.
        password (str): The password for the database connection.
        last_updated (str): The minimum value for the last_updated column.

    Returns:
        tuple: A tuple containing a dictionary with the sales order data and a string with the maximum value of the last_updated column.
    """
    try:
        conn = pg8000.native.Connection(
            user, host=host, database=db, port=port, password=password)
        try:
            if last_updated is None:
                data_query = '''SELECT * FROM sales_order ORDER BY last_updated;'''
            else:
                data_query = f'''SELECT * FROM sales_order WHERE last_updated > '{last_updated}' ORDER BY last_updated;'''

            rows = conn.run(data_query)
            columns = conn.columns
            column_titles = [c['name'] for c in columns]
            sales_order_data = format_to_dict(rows, column_titles)

            max_query = '''SELECT MAX(last_updated) FROM sales_order;'''
            max_last_updated = conn.run(max_query)[0][0]
            max_last_updated += timedelta(seconds=1)
            max_last_updated = max_last_updated.replace(microsecond=0)

            return sales_order_data, max_last_updated
        except Exception as e:
            logger.error(
                f'An error occurred while executing the SQL query: {e}')
    except Exception as e:
        logger.error(
            f'An error occurred while connecting to the database: {e}')
    finally:
        conn.close()


def get_staff_data(user, host, db, port, password, last_updated=None):
    """Retrieves staff data from a database.

    This function connects to a database using the provided
    parameters and retrieves staff data from the 'staff' table.
    The data is then formatted into a dictionary and returned.

    Args:
        user (str): The username for the database connection.
        host (str): The hostname for the database connection.
        db (str): The name of the database to connect to.
        port (int): The port number for the database connection.
        password (str): The password for the database connection.
        last_updated (str): The minimum value for the last_updated column.

    Returns:
        tuple: A tuple containing a dictionary with the staff data and a string with the maximum value of the last_updated column.
    """
    try:
        conn = pg8000.native.Connection(
            user, host=host, database=db, port=port, password=password)
        try:
            if last_updated is None:
                query = '''SELECT * FROM staff ORDER BY last_updated;'''
            else:
                query = f'''SELECT * FROM staff WHERE last_updated > '{last_updated}' ORDER BY last_updated;'''

            rows = conn.run(query)
            columns = conn.columns
            column_titles = [c['name'] for c in columns]
            staff_data = format_to_dict(rows, column_titles)

            max_query = '''SELECT MAX(last_updated) FROM staff;'''
            max_last_updated = conn.run(max_query)[0][0]
            max_last_updated += timedelta(seconds=1)
            max_last_updated = max_last_updated.replace(microsecond=0)

            return staff_data, max_last_updated
        except Exception as e:
            logger.error(
                f'An error occurred while executing the SQL query: {e}')
    except Exception as e:
        logger.error(
            f'An error occurred while connecting to the database: {e}')
    finally:
        conn.close()


def get_transaction_data(user, host, db, port, password, last_updated=None):
    """Retrieves transaction data from a database.

    This function connects to a database using the provided
    parameters and retrieves transaction data from the 'transaction'
    table. The data is then formatted into a dictionary and returned.

    Args:
        user (str): The username for the database connection.
        host (str): The hostname for the database connection.
        db (str): The name of the database to connect to.
        port (int): The port number for the database connection.
        password (str): The password for the database connection.
        last_updated (str): The minimum value for the last_updated column.

    Returns:
        tuple: A tuple containing a dictionary with the transaction data and a string with the maximum value of the last_updated column.
    """
    try:
        conn = pg8000.native.Connection(
            user, host=host, database=db, port=port, password=password)
        try:
            if last_updated is None:
                query = '''SELECT * FROM transaction ORDER BY last_updated;'''
            else:
                query = f'''SELECT * FROM transaction WHERE last_updated > '{last_updated}' ORDER BY last_updated;'''

            rows = conn.run(query)
            columns = conn.columns
            column_titles = [c['name'] for c in columns]
            transaction_data = format_to_dict(rows, column_titles)

            max_query = '''SELECT MAX(last_updated) FROM transaction;'''
            max_last_updated = conn.run(max_query)[0][0]
            max_last_updated += timedelta(seconds=1)
            max_last_updated = max_last_updated.replace(microsecond=0)

            return transaction_data, max_last_updated
        except Exception as e:
            logger.error(
                f'An error occurred while executing the SQL query: {e}')
    except Exception as e:
        logger.error(
            f'An error occurred while connecting to the database: {e}')
    finally:
        conn.close()
