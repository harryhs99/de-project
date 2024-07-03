from src.lambda_functions.processed_lambda import create_dim_date


def test_should_have_correct_columns():
    expected_columns = ['date_id', 'year', 'month', 'day',
                        'day_of_week', 'day_name', 'month_name', 'quarter']
    assert list(create_dim_date().columns) == expected_columns


def test_should_have_correct_data_types():
    expected_data_types = {
        'year': 'int32',
        'month': 'int32',
        'day': 'int32',
        'day_of_week': 'int32',
        'day_name': 'object',
        'month_name': 'object',
        'quarter': 'period[Q-DEC]'
    }
    data_frame = create_dim_date()
    for column, data_type in expected_data_types.items():
        assert data_frame[column].dtype == data_type
