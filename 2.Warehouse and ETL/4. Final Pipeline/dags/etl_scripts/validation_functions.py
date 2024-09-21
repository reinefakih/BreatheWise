# -- import statements -- #
import pandas as pd
import os
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

# -- code -- #

# checking if the table already exists in the database:
def check_table_exists(table_name: str, engine) -> bool:
    """Check if a table exists in the specified database schema.

    Args:
        table_name (str): Name of the table to check.
        engine: SQLAlchemy engine object to connect to the database.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    query = f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'lung_cancer_air_pollution_staging_area'
            AND table_name = '{table_name}';
        """

    result = pd.read_sql(query, engine).iloc[0, 0]
    return result == 1

def get_row_count(table_name: str, engine) -> int:
    """Gets the number of rows of a Table in a specified database.

    Args:
        table_name (str): Name of the table to check.
        engine: SQLAlchemy engine object to connect to the database.

    Returns:
        int: the number of rows in the database.
    """
    query = f"SELECT COUNT(*) FROM {table_name}"
    return pd.read_sql(query, engine).iloc[0, 0]


def check_year_exists(year_values, engine, table_name):
    """Check if the years already exist in the target table.

    Args:
        year_values (list): List of years to check.
        engine: SQLAlchemy engine object to connect to the database.
        table_name (str): The name of the target table.

    Returns:
        set: Set of years that already exist in the target table.
    """
    if not year_values:
        return set()  # Return empty set if no years to check
  
    # Format the placeholders for the number of years
    year_placeholders = ', '.join(['%s'] * len(year_values))

    # Construct the SQL query
    query = f"""
        SELECT DISTINCT year
        FROM {table_name}
        WHERE year IN ({year_placeholders})
    """
    # print(query)

    try:
        # Execute the query with parameters
        with engine.connect() as connection:
            existing_years = pd.read_sql(query, connection, params=tuple(year_values))
        # Return the set of existing years
        return set(existing_years['year'])
    
    except SQLAlchemyError as e:
        # Handle SQLAlchemy errors
        print(f"SQLAlchemyError: {e}")
        return set(['error'])
    
    except Exception as e:
        # Handle other errors
        print(f"Error checking existing years: {e}")
        return set(['error'])
    

def check_year_month_exists(year_month_values, engine, table_name):
    """Check if the year_month values already exist in the target table.

    Args:
        year_month_values (list): List of year_month values to check.
        engine: SQLAlchemy engine object to connect to the database.
        table_name (str): The name of the target table.

    Returns:
        set: Set of year_month values that already exist in the target table.
    """
    if not year_month_values:
        return set()  # Return empty set if no year_month values to check
  
    # Format the placeholders for the number of year_month values
    year_month_placeholders = ', '.join(['%s'] * len(year_month_values))

    # Construct the SQL query
    query = f"""
        SELECT DISTINCT `year_month`
        FROM {table_name}
        WHERE `year_month` IN ({year_month_placeholders})
        """
    print(query)

    try:
        # Execute the query with parameters
        existing_year_months = pd.read_sql(query, engine, params=tuple(year_month_values))

        # Return the set of existing year_month values
        return set(existing_year_months['year_month'])
    
    except SQLAlchemyError as e:
        # Handle SQLAlchemy errors
        print(f"SQLAlchemyError: {e}")
        return set(['error'])
    
    except Exception as e:
        # Handle other errors
        print(f"Error checking existing year_month values: {e}")
        return set(['error'])