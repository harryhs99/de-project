import datetime

rows = [
    [
        1,
        "GBP",
        datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
        datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
    ],
    [
        2,
        "USD",
        datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
        datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
    ],
    [
        3,
        "EUR",
        datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
        datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
    ],
]
columns = [
    {
        "table_oid": 16457,
        "column_attrnum": 1,
        "type_oid": 23,
        "type_size": 4,
        "type_modifier": -1,
        "format": 0,
        "name": "currency_id",
    },
    {
        "table_oid": 16457,
        "column_attrnum": 2,
        "type_oid": 1043,
        "type_size": -1,
        "type_modifier": 7,
        "format": 0,
        "name": "currency_code",
    },
    {
        "table_oid": 16457,
        "column_attrnum": 3,
        "type_oid": 1114,
        "type_size": 8,
        "type_modifier": 3,
        "format": 0,
        "name": "created_at",
    },
    {
        "table_oid": 16457,
        "column_attrnum": 4,
        "type_oid": 1114,
        "type_size": 8,
        "type_modifier": 3,
        "format": 0,
        "name": "last_updated",
    },
]
