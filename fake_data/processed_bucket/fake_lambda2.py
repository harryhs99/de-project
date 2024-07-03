
import json
import pandas as pd



# using fake data for now
with open("fake_data/ingestion_bucket/currency.json") as f:
    json_currency = json.load(f)
# create currency dataframe
df_currency = pd.DataFrame(json_currency["data"])
dim_currency = df_currency.iloc[:, :2]
dim_currency.set_index("currency_id", inplace=True)
dim_currency["currency_name"] = ["pound", "dollar", "euro"]
# convert to parquet
dim_currency.to_parquet("fake_data/processed_bucket/dim_currency.parquet")
# pd.read_parquet(dim_currency, engine="auto")


def create_dim_design():
    # using fake data
    with open("fake_data/ingestion_bucket/design.json") as f:
        json_design = json.load(f)
    # create currency dataframe
    df_design = pd.DataFrame(json_design["data"])
    dim_design = df_design[["design_id", "design_name", "file_location", "file_name"]]
    dim_design.set_index("design_id", inplace=True)
    # convert to parquet
    dim_design.to_parquet("fake_data/processed_bucket/dim_design.parquet")

create_dim_design()
# with open("fake_data/ingestion_bucket/department.json") as f:
#     json_department = json.load(f)
# df_department = pd.DataFrame(json_department["data"])

# with open("fake_data/ingestion_bucket/staff.json") as f:
#     json_staff = json.load(f)
# df_staff1 = pd.DataFrame(json_staff["data"])
# df_staff = df_staff1.merge(
#     df_department[["department_id", "department_name", "location"]],
#     on="department_id",
#     how="left",
# )
# dim_staff = df_staff.drop(["department_id", "created_at", "last_updated"], axis=1)
# dim_staff.set_index("staff_id", inplace=True)
# dim_staff.to_parquet("fake_data/processed_bucket/dim_staff.parquet")

# with open("fake_data/ingestion_bucket/address.json") as f:
#     json_location = json.load(f)
# df_location = pd.DataFrame(json_location["data"])
# dim_location = df_location[
#     [
#         "address_id",
#         "address_line_1",
#         "address_line_2",
#         "district",
#         "city",
#         "postal_code",
#         "country",
#         "phone",
#     ]
# ]
# dim_location.rename(columns={"address_id": "location_id"}, inplace=True)
# dim_location.set_index("location_id", inplace=True)
# dim_location.to_parquet("fake_data/processed_bucket/dim_location.parquet")

# with open("fake_data/ingestion_bucket/counterparty.json") as f:
#     json_counterparty = json.load(f)
# df_counterparty = pd.DataFrame(json_counterparty["data"])

# dim_counterparty = pd.merge(
#     df_counterparty,
#     df_location,
#     left_on="legal_address_id",
#     right_on="address_id",
#     how="left",
# )
# dim_counterparty = dim_counterparty[
#     [
#         "counterparty_id",
#         "counterparty_legal_name",
#         "address_line_1",
#         "address_line_2",
#         "district",
#         "city",
#         "postal_code",
#         "country",
#         "phone",
#     ]
# ]
# dim_counterparty.rename(
#     columns={
#         "address_line_1": "counterparty_legal_address_line_1",
#         "address_line_2": "counterparty_legal_address_line_2",
#         "district": "counterparty_legal_district",
#         "city": "counterparty_legal_city",
#         "postal_code": "counterparty_legal_postal_code",
#         "country": "counterparty_legal_country",
#         "phone": "counterparty_legal_phone_number",
#     },
#     inplace=True,
# )
# dim_counterparty.set_index("counterparty_id", inplace=True)
# dim_counterparty.to_parquet("fake_data/processed_bucket/dim_counterparty.parquet")

# date_range = pd.date_range(start="2022-01-01", end="2023-12-31", freq="D")
# dim_date = pd.date_range(start="2022-01-01", end="2023-12-31", freq="D")
# dim_date = pd.DataFrame(date_range, columns=["date"])
# dim_date["year"] = dim_date["date"].dt.year
# dim_date["month"] = dim_date["date"].dt.month
# dim_date["day"] = dim_date["date"].dt.day
# dim_date["day_of_week"] = dim_date["date"].dt.weekday
# dim_date["day_name"] = dim_date["date"].dt.strftime("%A")
# dim_date["month_name"] = dim_date["date"].dt.strftime("%B")
# dim_date["quarter"] = dim_date["date"].dt.to_period("Q")
# dim_date.set_index("date", inplace=True)
# dim_date.to_parquet("fake_data/processed_bucket/dim_date.parquet")

# with open("fake_data/ingestion_bucket/sales_order.json") as f:
#     json_sales_order = json.load(f)
# df_sales_order = pd.DataFrame(json_sales_order["data"])
# df_sales_order[["created_date", "created_time"]] = df_sales_order[
#     "created_at"
# ].str.split(", ", expand=True)
# df_sales_order[["last_updated_date", "last_updated_time"]] = df_sales_order[
#     "last_updated"
# ].str.split(", ", expand=True)
# fact_sales_order = df_sales_order[
#     [
#         "sales_order_id",
#         "created_date",
#         "created_time",
#         "last_updated_date",
#         "last_updated_time",
#         "staff_id",
#         "counterparty_id",
#         "units_sold",
#         "unit_price",
#         "currency_id",
#         "design_id",
#         "agreed_payment_date",
#         "agreed_delivery_date",
#         "agreed_delivery_location_id",
#     ]
# ]
# fact_sales_order["unit_price"] = pd.to_numeric(fact_sales_order["unit_price"])
# fact_sales_order["created_date"] = pd.to_datetime(fact_sales_order["created_date"])
# fact_sales_order["created_time"] = (
#     pd.to_datetime(fact_sales_order["created_time"], format="%H:%M:%S:%f")
#     .dt.floor("s")
#     .dt.time
# )
# fact_sales_order["last_updated_date"] = pd.to_datetime(
#     fact_sales_order["last_updated_date"]
# )
# fact_sales_order["last_updated_time"] = (
#     pd.to_datetime(fact_sales_order["last_updated_time"], format="%H:%M:%S:%f")
#     .dt.floor("s")
#     .dt.time
# )
# fact_sales_order["agreed_payment_date"] = pd.to_datetime(
#     fact_sales_order["agreed_payment_date"]
# )
# fact_sales_order["agreed_delivery_date"] = pd.to_datetime(
#     fact_sales_order["agreed_delivery_date"]
# )

# fact_sales_order.index.name = "sales_record_id"
# fact_sales_order.to_parquet("fake_data/processed_bucket/fact_sales_order.parquet")

# with open("fake_data/ingestion_bucket/transaction.json") as f:
#     json_transaction_data = json.load(f)
# df_transaction = pd.DataFrame(json_transaction_data["data"])
# df_transaction = df_transaction.drop(["created_at", "last_updated"], axis=1)
# df_transaction[["sales_order_id", "purchase_order_id"]] = (
#     df_transaction[["sales_order_id", "purchase_order_id"]].fillna(0).astype(int)
# )


# df_transaction.to_parquet("fake_data/processed_bucket/dim_transactions.parquet")

# # print(fact_sales_order.head())
# data_types = fact_sales_order.dtypes
# print(data_types)
# possible tests check there are no null values in the data. SQL tables are all set to [not null]
# b = fact_sales_order.isnull().any(axis=0)
# print(b)
# null_mask = fact_sales_order.isnull()
# null_rows = fact_sales_order[null_mask.any(axis=1)]
# print(null_mask)
# print(null_rows)
