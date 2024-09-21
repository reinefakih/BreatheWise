# -- import statements -- #
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as sa
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from validation_scripts import check_dim_unique_values
from air_quality_index_functions import calculate_aqi, classify_air_quality,o3_breakpoints, no2_breakpoints, so2_breakpoints, pm10_breakpoints, pm25_breakpoints
# -- code -- #

# creating the connection between python and mysql database

connection_url = f"mysql+pymysql://{os.environ['TEST_DB_USERNAME']}:{os.environ['TEST_DB_PASSWORD']}@{os.environ['TEST_DB_HOSTNAME']}/{os.environ['TEST_DB_DATABASE_NAME']}"
db_engine = create_engine(connection_url)

# initializing a session to manage database operations like rolling back changes if we faced an error

Session = sessionmaker(bind=db_engine)
session = Session()

# -- transformation scripts -- #

# FOR FACT TABLE:

# function to transform the air pollution df and lung cancer df and combine them into one!

def fact_transformation():
    
    try:
        # lung cancer df:

        lung = pd.read_sql("SELECT id, age, gender, country, diagnosis_date, end_treatment_date, cancer_stage, hypertension, asthma, cirrhosis, treatment_type, survived FROM lung_cancer", con=db_engine)

        lung['diagnosis_date'] = pd.to_datetime(lung['diagnosis_date'])
        lung['end_treatment_date'] = pd.to_datetime(lung['end_treatment_date'])

        lung['diagnosis_date'] = lung['diagnosis_date'].dt.strftime('%Y-%m')
        lung['end_treatment_date'] = lung['end_treatment_date'].dt.strftime('%Y-%m')

        lung['age'] = lung['age'].astype(int)
        country_codes = {
            "Latvia": "LV",
            "Malta": "MT",
            "Cyprus": "CY",
            "Denmark": "DK",
            "Greece": "GR",
            "Italy": "IT",
            "Belgium": "BE",
            "Czech Republic": "CZ",
            "Croatia": "HR",
            "Sweden": "SE",
            "Estonia": "EE",
            "Germany": "DE",
            "Finland": "FI",
            "Lithuania": "LT",
            "Spain": "ES",
            "Luxembourg": "LU",
            "Bulgaria": "BG",
            "Poland": "PL",
            "Romania": "RO",
            "Austria": "AT",
            "Slovakia": "SK",
            "Netherlands": "NL",
            "Ireland": "IE",
            "France": "FR",
            "Hungary": "HU",
            "Portugal": "PT",
            "Slovenia": "SI"
        }

        lung['CountryCode'] = lung['country'].map(country_codes)

        # air data:

        air_data = pd.read_sql("SELECT * FROM air_pollution", con=db_engine)
        air_data = air_data.pivot_table(index=['CountryCode', 'year_month'], columns='PollutantName', values='Value').reset_index()
        
        if 'BaP' in air_data.columns:
            air_data = air_data.drop(columns=['BaP'])
            print("Dropped BaP column")
        else:
            print('Column BaP not present already')
        
        # Calculate AQI for each pollutant
        air_data['AQI_O3'] = air_data['O3'].apply(calculate_aqi, breakpoints=o3_breakpoints)
        air_data['AQI_PM10'] = air_data['PM10'].apply(calculate_aqi, breakpoints=pm10_breakpoints)
        air_data['AQI_PM2.5'] = air_data['PM2.5'].apply(calculate_aqi, breakpoints=pm25_breakpoints)
        air_data['AQI_NO2'] = air_data['NO2'].apply(calculate_aqi, breakpoints=no2_breakpoints)
        air_data['AQI_SO2'] = air_data['SO2'].apply(calculate_aqi, breakpoints=so2_breakpoints)

        # Get the maximum AQI for each row
        air_data['Max_AQI'] = air_data[['AQI_O3', 'AQI_PM10', 'AQI_PM2.5', 'AQI_NO2', 'AQI_SO2']].max(axis=1)

        air_data['Air_Quality'] = air_data['Max_AQI'].apply(classify_air_quality)
        air_data = air_data.drop(['NO2', 'O3', 'PM10', 'PM2.5', 'SO2'], axis=1)
        air_data['year_month'] = air_data['year_month'].astype(str)

        merged_df = lung.merge(air_data, left_on=['CountryCode', 'diagnosis_date'], right_on=['CountryCode', 'year_month'], how='inner')
        final_table = merged_df[['id', 'age', 'gender', 'country', 'diagnosis_date', 'end_treatment_date', 'cancer_stage', 'hypertension', 'asthma', 'cirrhosis', 'treatment_type', 'survived', 'AQI_PM2.5', 'AQI_PM10',  'AQI_NO2', 'AQI_SO2', 'AQI_O3', 'Max_AQI', 'Air_Quality']]
        final_table = final_table.rename({'id': 'patient_id', 
                                    'age':'patient_age', 
                                    'gender' : 'patient_gender',
                                    'AQI_PM2.5': 'pm25_index',
                                    'AQI_PM10': 'pm10_index',
                                    'AQI_NO2': 'no2_index',
                                    'AQI_SO2': 'so2_index',
                                    'AQI_O3': 'o3_index',
                                    'Max_AQI': 'air_quality_index',
                                    'Air_Quality': 'air_quality'
                                    })
        
        return print(final_table)
    
    except Exception as e:
        print(f"Error: {e}")
        return None

fact_transformation()

# FOR DIMENSION TABLES:

# dim_regions:
def regions_tranformation():
    try:    
        df = pd.read_sql("SELECT * FROM country_regions", con=db_engine)
        
        region_id = {'Northern Europe' : 'NE', 
             'Southern Europe': 'SE', 
             'Western Europe': 'WE',
             'Central Europe': 'CE', 
             'Eastren Europe': 'EE', 
             'Southeastren Europe': 'SEE'
             }

        df['region_id'] = df['region'].map(region_id)

        rows = check_dim_unique_values('country_regions', 'region', df, 'region', db_engine)
        if rows:
            return print(df)
        
        elif rows == None:
            print('Error while doing the row check!')
            return None
        else: 
            raise ValueError(f"Row count mismatch after transformation")
    except Exception as e:
        print(f"Error: {e}")
        return None

def country_transformation():
    try:    
        df = pd.read_sql("SELECT DISTINCT country FROM lung_cancer", con=db_engine)

        countries = df['country'].unique().tolist()
        unique_countries_df = pd.DataFrame(countries, columns=['country'])
        
        country_codes = {
                            "Latvia": "LV",
                            "Malta": "MT",
                            "Cyprus": "CY",
                            "Denmark": "DK",
                            "Greece": "GR",
                            "Italy": "IT",
                            "Belgium": "BE",
                            "Czech Republic": "CZ",
                            "Croatia": "HR",
                            "Sweden": "SE",
                            "Estonia": "EE",
                            "Germany": "DE",
                            "Finland": "FI",
                            "Lithuania": "LT",
                            "Spain": "ES",
                            "Luxembourg": "LU",
                            "Bulgaria": "BG",
                            "Poland": "PL",
                            "Romania": "RO",
                            "Austria": "AT",
                            "Slovakia": "SK",
                            "Netherlands": "NL",
                            "Ireland": "IE",
                            "France": "FR",
                            "Hungary": "HU",
                            "Portugal": "PT",
                            "Slovenia": "SI"
                        }
        
        unique_countries_df['CountryCode'] = unique_countries_df['country'].map(country_codes)

        rows = check_dim_unique_values('lung_cancer', 'country', unique_countries_df, 'country', db_engine)
        
        if rows:
            return print(unique_countries_df)
        
        elif rows == None:
            print('Error while doing the row check!')
            return None
        else: 
            raise ValueError(f"Row count mismatch after transformation")
    
    except Exception as e:
        print(f"Error: {e}")  
        return None 

# def income_transformation():
#     try:
#         df = pd.read_sql("SELECT * FROM country_income", con=db_engine)
#         fdf = df[['country', 'Year', 'World Bank\'s income classification']]
        
#         country_codes = {
#                 "Latvia": "LV",
#                 "Malta": "MT",
#                 "Cyprus": "CY",
#                 "Denmark": "DK",
#                 "Greece": "GR",
#                 "Italy": "IT",
#                 "Belgium": "BE",
#                 "Czech Republic": "CZ",
#                 "Croatia": "HR",
#                 "Sweden": "SE",
#                 "Estonia": "EE",
#                 "Germany": "DE",
#                 "Finland": "FI",
#                 "Lithuania": "LT",
#                 "Spain": "ES",
#                 "Luxembourg": "LU",
#                 "Bulgaria": "BG",
#                 "Poland": "PL",
#                 "Romania": "RO",
#                 "Austria": "AT",
#                 "Slovakia": "SK",
#                 "Netherlands": "NL",
#                 "Ireland": "IE",
#                 "France": "FR",
#                 "Hungary": "HU",
#                 "Portugal": "PT",
#                 "Slovenia": "SI"
#             }

#         fdf['country_code'] = fdf['country'].map(country_codes)
        
#         income_classification = {
#             'Low Income': 'LIC',
#             'Lower Middle Income': 'LMC',
#             'Upper Middle Income': 'UMC',
#             'High Income': 'HIC'
#         }
#         income_ranges = {
#             'LIC': '≤ $1,085',
#             'LMC': '$1,086 - $4,255',
#             'UMC': '$4,256 - $13,205',
#             'HIC': '≥ $13,206'
#         }
#         classification_name_fix = {
#             'High-income countries'	: 'High Income',
#             'Low-income countries': 'Low Income',
#             'Lower-middle-income countries': 'Lower Middle Income',
#             'Upper-middle-income countries': 'Upper Middle Income',
#         }

#         fdf['classification'] = fdf['World Bank\'s income classification'].map(classification_name_fix)

#         fdf['classification_id'] = fdf['classification'].map(income_classification)

#         fdf['income_range'] = fdf['classification_id'].map(income_ranges)

#         fdf = fdf.drop(['World Bank\'s income classification'], axis=1)
        
#         rows = check_dim_unique_values('country_income', '`World Bank\'s income classification`', fdf, 'classification', db_engine)
        
#         if rows:
#             return fdf
        
#         elif rows == None:
#             print('Error while doing the row check!')
#             return None
#         else: 
#             raise ValueError(f"Row count mismatch after transformation")

#     except Exception as e:
#         print(f"Error: {e}")
#         return None

def treatment_type_transformation():
    try:    
        df = pd.read_sql("SELECT DISTINCT treatment_type FROM lung_cancer", con=db_engine)

        treatments = df['treatment_type'].unique().tolist()
        treatment_df = pd.DataFrame(treatments, columns=['treatment_type'])

        rows = check_dim_unique_values('lung_cancer', 'treatment_type', treatment_df, 'treatment_type', db_engine)
        
        if rows:
            return print(treatment_df)
        
        elif rows == None:
            print('Error while doing the row check!')
            return None
        else: 
            raise ValueError(f"Row count mismatch after transformation")
    
    except Exception as e:
        print(f"Error: {e}")  
        return None

def cancer_stage_transformation():
    try:    
        df = pd.read_sql("SELECT DISTINCT cancer_stage FROM lung_cancer", con=db_engine)

        stage = df['cancer_stage'].unique().tolist()
        stage_df = pd.DataFrame(stage, columns=['cancer_stage'])
        stage_ids = {
            'Stage I': 's1',
            'Stage II': 's2',
            'Stage III': 's3',
            'Stage IV': 's4',
        }

        stage_df['cancer_stage_id'] = stage_df['cancer_stage'].map(stage_ids)

        rows = check_dim_unique_values('lung_cancer', 'cancer_stage', stage_df, 'cancer_stage', db_engine)
        
        if rows:
            return print(stage_df)
        
        elif rows == None:
            print('Error while doing the row check!')
            return None
        else: 
            raise ValueError(f"Row count mismatch after transformation")
    
    except Exception as e:
        print(f"Error: {e}")  
        return None  

def dates_transformation():
    try:
        diagnosis_date = pd.read_sql("SELECT DISTINCT diagnosis_date FROM lung_cancer", con=db_engine)
        end_treatment_date = pd.read_sql("SELECT DISTINCT end_treatment_date FROM lung_cancer", con=db_engine)

        diagnosis_date['diagnosis_date'] = pd.to_datetime(diagnosis_date['diagnosis_date'])
        end_treatment_date['end_treatment_date'] = pd.to_datetime(end_treatment_date['end_treatment_date'])

        diagnosis_date['date'] = diagnosis_date['diagnosis_date'].dt.strftime('%Y-%m')
        end_treatment_date['date'] = end_treatment_date['end_treatment_date'].dt.strftime('%Y-%m')

        dates = pd.concat([diagnosis_date['date'],end_treatment_date['date']], axis=0)

        unique_dates = dates.unique().tolist()
        dates_df = pd.DataFrame(unique_dates, columns=['dates'])

        dd_validation = check_dim_unique_values('lung_cancer', 'diagnosis_date', diagnosis_date, 'diagnosis_date', db_engine)
        etd_validation = check_dim_unique_values('lung_cancer', 'end_treatment_date', end_treatment_date, 'end_treatment_date', db_engine)

        if dd_validation and etd_validation:
            return print(dates_df)
        
        elif (dd_validation== None or etd_validation== None):
            print('Error while doing the row check!')
            return None
        
        else: 
            raise ValueError(f"Row count mismatch after transformation")
    except Exception as e:
        print(f"Error: {e}")
        return None

def air_quality_quantities_transformation():
    try:
        data = {
        'AQI Range': ['0-50', '51-100', '101-150', '151-200', '201-300', '301-500'],
        'Category': ['Good', 'Moderate', 'Unhealthy for Sensitive Groups', 'Unhealthy', 'Very Unhealthy', 'Hazardous'],
        }

        # Create a DataFrame
        aqi_df = pd.DataFrame(data)
        return print(aqi_df)

    except Exception as e:
        print(f"Error: {e}")
        return None

# FOR LOGICAL DATAMARTS:

