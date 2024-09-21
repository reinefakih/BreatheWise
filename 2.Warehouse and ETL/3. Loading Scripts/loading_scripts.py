import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as sa
import os
import numpy as np
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import Integer, Float, String, DateTime
from transformation_scripts import fact_transformation, regions_tranformation, country_transformation, treatment_type_transformation, cancer_stage_transformation, air_quality_quantities_transformation, dates_transformation

warehouse_url = f"mysql+pymysql://{os.environ['TEST_DB_USERNAME']}:{os.environ['TEST_DB_PASSWORD']}@{os.environ['TEST_DB_HOSTNAME']}/lung_cancer_air_pollution_warehouse_test"
warehouse_engine = create_engine(warehouse_url)

# initializing a session to manage database operations like rolling back changes if we faced an error

Session = sessionmaker(bind=warehouse_url)
session = Session()

def load_into_dim_treatment_types(df):
    try:
        if df is not None and isinstance(df, pd.DataFrame):
            with warehouse_engine.begin() as connection:
                try:
                    connection.execute(sa.text("TRUNCATE TABLE dim_treatment_types;"))
                    print("Truncating dim_treatment_types table...")
                    
                    insert_sql = sa.text("""
                    INSERT INTO dim_treatment_types (treatment)
                    VALUES (:treatment_type);
                    """)
                    
                    # Iterate over DataFrame rows
                    for index, row in df.iterrows():
                        connection.execute(insert_sql, {'treatment_type': row['treatment_type']})
                        print(f"Inserted row: {row['treatment_type']}")
                    print('dim_treatment_types laoded successfully!')
                    session.commit()
                except Exception as e:
                    session.rollback()
 
        else:
            session.rollback()
            raise ValueError(f"Did not recieve a dataframe to Load") 
        
    except Exception as e:
        print(f"Error: {e}")

# load_into_dim_treatment_types(treatment_type_transformation())

def load_into_dim_cancer_stages(df):
    try:
        if df is not None and isinstance(df, pd.DataFrame):
            with warehouse_engine.begin() as connection:
                try:
                    connection.execute(sa.text("TRUNCATE TABLE dim_cancer_stages;"))
                    print("Truncating dim_cancer_stages table...")
                    
                    insert_sql = sa.text("""
                    INSERT INTO dim_cancer_stages (cancer_stage_id, cancer_stage)
                    VALUES (:cancer_stage_id, :cancer_stage);
                    """)
                    
                    # Iterate over DataFrame rows
                    for index, row in df.iterrows():
                        connection.execute(insert_sql, {'cancer_stage_id':row['cancer_stage_id'],'cancer_stage': row['cancer_stage']})
                        print(f"Inserted row: {row['cancer_stage_id']}, {row['cancer_stage']}")
                    print('dim_cancer_stages laoded successfully!')
                    session.commit()
                except Exception as e:
                    session.rollback()
 
        else:
            session.rollback()
            raise ValueError(f"Did not recieve a dataframe to Load") 
        
    except Exception as e:
        print(f"Error: {e}")

# load_into_dim_cancer_stages(cancer_stage_transformation())

def load_into_dim_air_qualities(df):
    try:
        if df is not None and isinstance(df, pd.DataFrame):
            with warehouse_engine.begin() as connection:
                try:
                    connection.execute(sa.text("TRUNCATE TABLE dim_air_qualities;"))
                    print("Truncating dim_air_qualities table...")
                    
                    insert_sql = sa.text("""
                    INSERT INTO dim_air_qualities (air_quality, air_quality_range)
                    VALUES (:air_quality, :air_quality_range);
                    """)
                    
                    # Iterate over DataFrame rows
                    for index, row in df.iterrows():
                        connection.execute(insert_sql, {'air_quality':row['Category'], 'air_quality_range': row['AQI Range']})
                        print(f"Inserted row: {row['Category']}, {row['AQI Range']}")
                    print('dim_air_qualities laoded successfully!')
                    session.commit()
                except Exception as e:
                    session.rollback()
 
        else:
            session.rollback()
            raise ValueError(f"Did not recieve a dataframe to Load") 
        
    except Exception as e:
        print(f"Error: {e}")

# load_into_dim_air_qualities(air_quality_quantities_transformation())

def load_into_dim_regions(df):
    try:
        if df is not None and isinstance(df, pd.DataFrame):
            with warehouse_engine.begin() as connection:
                # print(df)
                try:
                    print("Extracting unique regions...")
                    unique_regions_df = df[['region_id', 'region']].drop_duplicates()
                    # print(unique_regions_df)

                    connection.execute(sa.text("TRUNCATE TABLE dim_regions;"))
                    print("Truncating dim_regions table...")
                    
                    insert_sql = sa.text("""
                    INSERT INTO dim_regions (region_id, region_name)
                    VALUES (:region_id, :region);
                    """)

                    # Iterate over DataFrame rows
                    try:
                        for index, row in unique_regions_df.iterrows():
                            connection.execute(insert_sql, {'region_id': row['region_id'], 'region': row['region']})
                            print(f"Inserted row: {row['region_id']}, {row['region']}")
                        print('dim_regions laoded successfully!')
                        session.commit()
                    except Exception as e:
                        print(e)
                
                except Exception as e:
                    session.rollback()

        else:
            session.rollback()
            raise ValueError(f"Did not recieve a dataframe to Load") 
        
    except Exception as e:
        print(f"Error: {e}")

# load_into_dim_regions(regions_tranformation())

def load_into_dim_countries(df):
    try:
        if df is not None and isinstance(df, pd.DataFrame):
            with warehouse_engine.begin() as connection:
                try:
                    # print('enetring')
                    result = connection.execute(sa.text("SELECT * FROM dim_regions"))
                    regions = result.all()
                    regions_df = pd.DataFrame(regions, columns=result.keys())
                    # print(regions_df)

                    country_regions = df.merge(regions_df, left_on='region', right_on='region_name', how='inner')

                    connection.execute(sa.text("TRUNCATE TABLE dim_countries;"))
                    print("Truncating dim_countries table...")
                    
                    insert_sql = sa.text("""
                    INSERT INTO dim_countries (country_code, country_name, region_id)
                    VALUES (:country_code, :country_name, :region_id);
                    """)
                    
                    # Iterate over DataFrame rows
                    for index, row in country_regions.iterrows():
                        connection.execute(insert_sql, {'country_code':row['CountryCode'], 'country_name': row['country'], 'region_id': row['region_id']})
                        print(f"Inserted row: {row['CountryCode']}, {row['country']}, {row['region_id']}")
                    print('dim_countries laoded successfully!')
                    session.commit()
                except Exception as e:
                    session.rollback()

        else:
            session.rollback()
            raise ValueError(f"Did not recieve a dataframe to Load") 
        
    except Exception as e:
        print(f"Error: {e}")

# load_into_dim_countries(country_transformation())

def load_into_dim_dates(df):
    try:
        if df is not None and isinstance(df, pd.DataFrame):
            with warehouse_engine.begin() as connection:
                # print(df)
                try:

                    connection.execute(sa.text("TRUNCATE TABLE dim_dates;"))
                    print("Truncating dim_dates table...")
                    
                    insert_sql = sa.text("""
                    INSERT INTO dim_dates (date, year, month, quarter)
                    VALUES (:date, :year, :month, :quarter);
                    """)

                    # Iterate over DataFrame rows
                    try:
                        for index, row in df.iterrows():
                            connection.execute(insert_sql, {'date': row['date'], 'year': row['year'], 'month': row['month'], 'quarter': row['quarter']})
                            print(f"Inserted row: {row['date']}, {row['year']}, {row['month']}, {row['quarter']}")
                        print('dim_dates laoded successfully!')
                        session.commit()
                    except Exception as e:
                        print(e)
                
                except Exception as e:
                    session.rollback()

        else:
            session.rollback()
            raise ValueError(f"Did not recieve a dataframe to Load") 
        
    except Exception as e:
        print(f"Error: {e}")

# load_into_dim_dates(dates_transformation())

def load_into_fact_table(df):
    try:
        if df is not None and isinstance(df, pd.DataFrame):
            with warehouse_engine.begin() as connection:
                try:
                    diagnosis_date_ids = connection.execute(sa.text("SELECT date_id, date FROM dim_dates"))
                    end_treament_ids = connection.execute(sa.text("SELECT date_id, date FROM dim_dates"))
                    treament_type_ids = connection.execute(sa.text("SELECT * FROM dim_treatment_types"))
                    cancer_stage_ids = connection.execute(sa.text("SELECT * FROM dim_cancer_stages"))
                    air_quality_ids = connection.execute(sa.text("SELECT air_quality_id, air_quality FROM dim_air_qualities"))
                    country_codes = connection.execute(sa.text("SELECT country_code, country_name FROM dim_countries"))

                    dd = diagnosis_date_ids.all()
                    dd_df = pd.DataFrame(dd, columns=diagnosis_date_ids.keys())

                    etd = end_treament_ids.all()
                    etd_df = pd.DataFrame(etd, columns=end_treament_ids.keys())

                    tt = treament_type_ids.all()
                    tt_df = pd.DataFrame(tt, columns=treament_type_ids.keys())

                    cs = cancer_stage_ids.all()
                    cs_df = pd.DataFrame(cs, columns=cancer_stage_ids.keys())

                    aq = air_quality_ids.all()
                    aq_df = pd.DataFrame(aq, columns=air_quality_ids.keys())

                    cc = country_codes.all()
                    cc_df = pd.DataFrame(cc, columns=country_codes.keys())

                    result_df = df.merge(dd_df, left_on='diagnosis_date', right_on='date', how='inner') \
                                    .merge(etd_df, left_on='end_treatment_date', right_on='date', how='inner') \
                                    .merge(tt_df, left_on='treatment_type', right_on='treatment', how='inner') \
                                    .merge(cs_df, on='cancer_stage', how='inner') \
                                    .merge(aq_df, left_on='air_quality', right_on='air_quality', how='inner') \
                                    .merge(cc_df, left_on='country', right_on='country_name', how='inner')
                    
                    final_df = result_df[['patient_id', 'patient_age', 'patient_gender', 'country_code', 'date_id_x', 'date_id_y', 'cancer_stage_id', 'hypertension', 'asthma', 'cirrhosis', 'treatment_type_id', 'survived', 'pm25_index', 'pm10_index', 'no2_index', 'so2_index', 'o3_index', 'air_quality_index', 'air_quality_id']]
                    final_df = final_df.rename(columns={'date_id_x': 'diagnosis_date_id', 'date_id_y': 'end_treatment_date_id'})
                    final_df = final_df.replace({np.nan: None})            

                    insert_sql = sa.text("""
                    INSERT INTO fact_patient_air_information (
                        patient_id, patient_age, patient_gender, country_code, diagnosis_date_id, end_of_treatment_date_id,
                        cancer_stage_id, hypertension, asthma, cirrhosis, treatment_type_id, survived,
                        pm25_index, pm10_index, no2_index, so2_index, o3_index, air_quality_index, air_quality_id
                    )
                    VALUES (
                        :patient_id, :patient_age, :patient_gender, :country_code, :diagnosis_date_id, :end_treatment_date_id,
                        :cancer_stage_id, :hypertension, :asthma, :cirrhosis, :treatment_type_id, :survived,
                        :pm25_index, :pm10_index, :no2_index, :so2_index, :o3_index, :air_quality_index, :air_quality_id
                    );
                    """)

                    # Iterate over DataFrame rows
                    try:
                        for index, row in final_df.iterrows():
                            connection.execute(insert_sql, {
                                'patient_id': row['patient_id'],
                                'patient_age': row['patient_age'],
                                'patient_gender': row['patient_gender'],
                                'country_code': row['country_code'],
                                'diagnosis_date_id': row['diagnosis_date_id'],
                                'end_treatment_date_id': row['end_treatment_date_id'],
                                'cancer_stage_id': row['cancer_stage_id'],
                                'hypertension': row['hypertension'],
                                'asthma': row['asthma'],
                                'cirrhosis': row['cirrhosis'],
                                'treatment_type_id': row['treatment_type_id'],
                                'survived': row['survived'],
                                'pm25_index': row['pm25_index'],
                                'pm10_index': row['pm10_index'],
                                'no2_index': row['no2_index'],
                                'so2_index': row['so2_index'],
                                'o3_index': row['o3_index'],
                                'air_quality_index': row['air_quality_index'],
                                'air_quality_id': row['air_quality_id']
                            })
                        print(f"Inserted {len(final_df)} rows")
                        print('fact_table loaded successfully!')
                    except Exception as e:
                        print(f"Error during insertion: {e}")

                
                except Exception as e:
                    session.rollback()

        else:
            session.rollback()
            raise ValueError(f"Did not recieve a dataframe to Load") 
        
    except Exception as e:
        print(f"Error: {e}")

load_into_fact_table(fact_transformation())