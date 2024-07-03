# !/bin/bash

export PYTHONPATH="$PWD"

PIP="pip"


# python true_data_from_aws/processed_data/test_parquet.py
# python fake_data/processed_bucket/fake_lambda2.py
# pytest /Users/davidluke/northcoders/de-project-specification/test/test_ingestion_lambda/test_ingestion_lambda.py

# python src/lambda_functions/ingestion_lambda.py
# python src/lambda_functions/processed_lambda.py
python src/lambda_functions/warehouse_lambda.py


