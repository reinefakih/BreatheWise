# -- import statements -- #
import pandas as pd
from sqlalchemy import create_engine
import os
from validation_functions import check_table_exists, get_row_count, check_year_exists, check_year_month_exists
from sqlalchemy.orm import sessionmaker

# -- code -- #

# creating the connection between python and mysql database

connection_url = f"mysql+pymysql://{os.environ['TEST_DB_USERNAME']}:{os.environ['TEST_DB_PASSWORD']}@{os.environ['TEST_DB_HOSTNAME']}/{os.environ['TEST_DB_DATABASE_NAME']}"
db_engine = create_engine(connection_url)

# initializing a session to manage database operations like rolling back changes if we faced an error

Session = sessionmaker(bind=db_engine)
session = Session()

# -- extracting scripts -- #

# extracting lung cancer data:
def extract_lung_cancer_data(csv_file: str):
    """ Extract data from CSV file related to lung cancer and insert it into the staging area table for lung cancer

    Args:
        csv_file (str): CSV file to be inserted into the Staging Area
    """
    # try to read the csv file:
    try:
        
        data = pd.read_csv(f"0.1.Data Used\\cancer data\\{csv_file}") # reading the df
        print(f"The current data has: {data.shape[0]} rows and {data.shape[1]} columns") # printing the # of rows and columns in the df for the sake of validation

        years = data['year'].unique().tolist() # getting the list of years in out data so we can use it in checking if the data inserted is already there or not

        table_exists = check_table_exists('lung_cancer', db_engine) # checking if the lung cancer table exists so we can append new data to it

        if table_exists: # if the table exists, continue to check if the data we are inserting is already there
            
            existing_years = check_year_exists(years, db_engine, 'lung_cancer') # check that the data we are inserting is not already in the table

            if existing_years != {'error'}: # if no error occurs during the data validation
                data_final = data[~data['year'].isin(existing_years)] # get the data we only want to insert
            
                count_of_rows_to_extract = len(data_final) # get the number of rows to extract
                
                if count_of_rows_to_extract > 0: # if there is new data to extract continue as is
                    try:
                        initial_target_row_count = get_row_count('lung_cancer', db_engine)

                        data_final.to_sql('lung_cancer', con= session.bind, if_exists='append', index=False)

                        final_target_row_count = get_row_count('lung_cancer', db_engine)

                        rows_inserted = final_target_row_count - initial_target_row_count

                        if rows_inserted == count_of_rows_to_extract:
                            session.commit()
                            print(f"Data inserted successfully! {count_of_rows_to_extract} rows have been inserted!")
                            

                        else: 
                            session.rollback()
                            raise ValueError(f"Row count mismatch after insertion: Inserted={rows_inserted}, Expected={count_of_rows_to_extract}")

                    except Exception as e:
                        session.rollback()
                        print(f"Error: {e}")
                
                else: # if no new data to extract, print that there are no new records to extract
                    session.commit()
                    print("No new records to extract!")
            else:
                print("Error while checking for years!")
                session.rollback()
        else: # if the table does not exist continue to making the table 
            count_of_rows_to_extract = len(data)
            data.to_sql('lung_cancer', con= session.bind, if_exists='append', index=False)
            inserted_rows = get_row_count('lung_cancer', db_engine) # get the number of rows inserted so we can validate that the number of rows we iserted are the same number as those in the source

            if inserted_rows == count_of_rows_to_extract: # if the rows are the same number commit the transaction
                session.commit()
                print(f"Data inserted successfully! {count_of_rows_to_extract} rows have been inserted!")
        
            else: # if not rollback the transaction and raise an error
                session.rollback()
                raise ValueError(f"Row count mismatch after insertion: Inserted={inserted_rows}, Expected={count_of_rows_to_extract}")
    
    # if an error happens while reading the csv, the error will be returned and the process stopped!
    except Exception as e:
        print(f"Error: {e}")
   

# extracting country region data:
# here we don't care to see if the table already exists as we are replacing the table with every extraction

def extract_country_region_data(csv_file):
    
    try:
        data = pd.read_csv(f"0.1.Data Used\\{csv_file}")
        print(f"The current data has: {data.shape[0]} rows and {data.shape[1]} columns")
        count_of_rows_to_extract = len(data)
        try:
            data.to_sql('country_regions', con= session.bind, if_exists='replace', index=False)
            
            query = f"SELECT COUNT(*) FROM country_regions"
            inserted_rows = pd.read_sql(query, db_engine).iloc[0, 0]

            if inserted_rows == count_of_rows_to_extract:
                print(f"Data inserted successfully! {count_of_rows_to_extract} rows have been inserted!")
                session.commit()
            else:
                session.rollback()
                raise ValueError(f"Row count mismatch after insertion: Inserted={inserted_rows}, Expected={count_of_rows_to_extract}")
            
        except Exception as e:
            session.rollback()
            print(f"Error: {e}")

    except Exception as e:
        print(f"Error: {e}") 



# extracting income data:
# def extract_income_data(csv_file: str):
#     """ Extract data from CSV file related to country incomes and insert it into the staging area table for country income

#     Args:
#         csv_file (str): CSV file to be inserted into the Staging Area
#     """
#     # try to read the csv file:
#     try:
        
#         data = pd.read_csv(f"0.1.Data Used\\income data\\{csv_file}") # reading the df
#         print(f"The current data has: {data.shape[0]} rows and {data.shape[1]} columns") # printing the # of rows and columns in the df for the sake of validation

#         years = data['Year'].unique().tolist() # getting the list of years in out data so we can use it in checking if the data inserted is already there or not

#         table_exists = check_table_exists('country_income', db_engine) # checking if the lung cancer table exists so we can append new data to it

#         if table_exists: # if the table exists, continue to check if the data we are inserting is already there
            
#             existing_years = check_year_exists(years, db_engine, 'country_income') # check that the data we are inserting is not already in the table

#             if existing_years != {'error'}: # if no error occurs during the data validation
#                 data_final = data[~data['Year'].isin(existing_years)] # get the data we only want to insert
            
#                 count_of_rows_to_extract = len(data_final) # get the number of rows to extract
                
#                 if count_of_rows_to_extract > 0: # if there is new data to extract continue as is
#                     try:
#                         initial_target_row_count = get_row_count('country_income', db_engine)

#                         data_final.to_sql('country_income', con= session.bind, if_exists='append', index=False)

#                         final_target_row_count = get_row_count('country_income', db_engine)

#                         rows_inserted = final_target_row_count - initial_target_row_count

#                         if rows_inserted == count_of_rows_to_extract:
#                             session.commit()
#                             print(f"Data inserted successfully! {count_of_rows_to_extract} rows have been inserted!")
                            

#                         else: 
#                             session.rollback()
#                             raise ValueError(f"Row count mismatch after insertion: Inserted={rows_inserted}, Expected={count_of_rows_to_extract}")

#                     except Exception as e:
#                         session.rollback()
#                         print(f"Error: {e}")
                
#                 else: # if no new data to extract, print that there are no new records to extract
#                     session.commit()
#                     print("No new records to extract!")
#             else:
#                 print("Error while checking for years!")
#                 session.rollback()
#         else: # if the table does not exist continue to making the table 
#             count_of_rows_to_extract = len(data)
#             data.to_sql('country_income', con= session.bind, if_exists='append', index=False)
#             inserted_rows = get_row_count('country_income', db_engine) # get the number of rows inserted so we can validate that the number of rows we iserted are the same number as those in the source

#             if inserted_rows == count_of_rows_to_extract: # if the rows are the same number commit the transaction
#                 session.commit()
#                 print(f"Data inserted successfully! {count_of_rows_to_extract} rows have been inserted!")
        
#             else: # if not rollback the transaction and raise an error
#                 session.rollback()
#                 raise ValueError(f"Row count mismatch after insertion: Inserted={inserted_rows}, Expected={count_of_rows_to_extract}")
    
#     # if an error happens while reading the csv, the error will be returned and the process stopped!
#     except Exception as e:
#         print(f"Error: {e}")


# extracting air pollution data:
def extract_air_pollution_data(parquet_file: str):
    """ Extract data from Parquet file related to air pollution and insert it into the staging area table for air pollution

    Args:
        parquet_file (str): Parquet file to be inserted into the Staging Area
    """
    # try to read the parquet file:
    try:
        data = pd.read_parquet(f"0.1.Data Used\\air data\\{parquet_file}") # reading the df
        print(f"The current data has: {data.shape[0]} rows and {data.shape[1]} columns") # printing the # of rows and columns in the df for the sake of validation

        year_month = (data['year_month'].astype(str)).unique().tolist() # getting the list of years in out data so we can use it in checking if the data inserted is already there or not
        # print(tuple(year_month))
        table_exists = check_table_exists('air_pollution', db_engine) # checking if the lung cancer table exists so we can append new data to it

        if table_exists: # if the table exists, continue to check if the data we are inserting is already there
            
            existing_years = check_year_month_exists(year_month, db_engine, 'air_pollution') # check that the data we are inserting is not already in the table

            if existing_years != {'error'}: # if no error occurs during the data validation
                data_final = data[~(data['year_month'].astype(str)).isin(existing_years)] # get the data we only want to insert
            
                count_of_rows_to_extract = len(data_final) # get the number of rows to extract
                
                if count_of_rows_to_extract > 0: # if there is new data to extract continue as is
                    try:
                        initial_target_row_count = get_row_count('air_pollution', db_engine)

                        data_final.to_sql('air_pollution', con= session.bind, if_exists='append', index=False)

                        final_target_row_count = get_row_count('air_pollution', db_engine)

                        rows_inserted = final_target_row_count - initial_target_row_count

                        if rows_inserted == count_of_rows_to_extract:
                            session.commit()
                            print(f"Data inserted successfully! {count_of_rows_to_extract} rows have been inserted!")
                            

                        else: 
                            session.rollback()
                            raise ValueError(f"Row count mismatch after insertion: Inserted={rows_inserted}, Expected={count_of_rows_to_extract}")

                    except Exception as e:
                        session.rollback()
                        print(f"Error: {e}")
                
                else: # if no new data to extract, print that there are no new records to extract
                    session.commit()
                    print("No new records to extract!")
            else:
                print("Error while checking for years!")
                session.rollback()
        
        else: # if the table does not exist continue to making the table 
            count_of_rows_to_extract = len(data)
            data.to_sql('air_pollution', con= session.bind, if_exists='fail', index=False)
            inserted_rows = get_row_count('air_pollution', db_engine) # get the number of rows inserted so we can validate that the number of rows we iserted are the same number as those in the source

            if inserted_rows == count_of_rows_to_extract: # if the rows are the same number commit the transaction
                session.commit()
                print(f"Data inserted successfully! {count_of_rows_to_extract} rows have been inserted!")
        
            else: # if not rollback the transaction and raise an error
                session.rollback()
                raise ValueError(f"Row count mismatch after insertion: Inserted={inserted_rows}, Expected={count_of_rows_to_extract}")
    
    # if an error happens while reading the parquet, the error will be returned and the process stopped!
    except Exception as e:
        print(f"Error: {e}")

