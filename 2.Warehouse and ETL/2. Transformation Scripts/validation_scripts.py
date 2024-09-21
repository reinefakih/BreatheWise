import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as sa
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError


def check_dim_unique_values(table_name, column, transformed_df, df_column,engine):
    try:
        raw_data = pd.read_sql(f"SELECT DISTINCT {column} FROM {table_name}", con=engine)
        len_transformed_data = len(transformed_df[df_column].unique())
        len_raw = len(raw_data)
        print(len_transformed_data, len_raw)

        return len_raw == len_transformed_data
    
    except Exception as e:
        print(e)
        return None