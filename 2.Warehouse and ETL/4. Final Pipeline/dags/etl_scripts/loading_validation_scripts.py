def validate_fact_table_dtypes(df):
    expected_schema = {
        'patient_id': 'int64', # integer
        'patient_age': 'int64', # integer
        'pateint_gender': 'object', # varchar
        'country_code': 'object', # varchar
        'diagnosis_date_id': 'int64', # integer
        'end_of_treament_date_id': 'int64', # integer
        'cancer_stage_id': 'object', # varchar
        'hypertension': 'int64', # tinyint
        'asthma': 'int64', # tinyint
        'cirrhosis': 'int64', # tinyint
        'treatment_type_id': 'int64', # integer
        'survived': 'int64', # tinyint
        'pm25_index': 'float64', # float
        'pm10_index': 'float64', # float
        'no2_index': 'float64', # float
        'so2_index': 'float64', # float
        'o3_index': 'float64', # float
        'air_quality_index': 'float64', # float
        'air_quality_id': 'int64', # integer
    }
    for column, dtype in expected_schema.items():
        df[column] = df[column].astype(dtype)
