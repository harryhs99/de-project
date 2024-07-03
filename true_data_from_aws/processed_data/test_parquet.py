import pandas as pd

df = pd.read_parquet(
    'true_data_from_aws/processed_data/fact_sales_order.parquet')

print(df.head())
