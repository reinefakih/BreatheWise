from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import logging

logger = logging.getLogger(__name__)


@dag(
    dag_id="prepare_file_info_dag",
    start_date=datetime(year=2024, month=1, day=1, hour=9, minute=0),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }
)
def prepare_file_info():

    file_info_list = [
        {
            "air_pollution": "2024.parquet",
            "country_region": "country_region.csv",
            "lung_cancer": "cancer_2024.csv"
        }
    ]

    # Loop through each file info and trigger the downstream DAG
    for i, file_info in enumerate(file_info_list):
        trigger_pipeline = TriggerDagRunOperator(
            task_id=f"trigger_pipeline_dag_{i}",
            trigger_dag_id="etl",  # Downstream DAG ID
            conf={"files_info": file_info},  # Pass each file info to the downstream DAG
            wait_for_completion=False  # You can set to True if you'd like to wait
        )

prepare_file_info()

@dag(
    dag_id="etl",
    start_date=datetime(year=2024, month=1, day=1, hour=9, minute=0),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5)}
)
def pipeline():
        
    @task
    def extract_air_pollution():
        from etl_scripts.cleaned_extracting_scripts import extract_air_pollution_data
        extract_air_pollution_data("2024.parquet")
        print("extract_air_pollution_data DONE!")
        logger.log(logging.INFO,"extract_air_pollution_data DONE!")
    
    @task
    def extract_country_region():
        from etl_scripts.cleaned_extracting_scripts import extract_country_region_data
        extract_country_region_data("country_region.csv")
        logger.log(logging.INFO,"extract_air_pollution_data DONE!")
    
    @task
    def extract_lung_cancer():
        from etl_scripts.cleaned_extracting_scripts import extract_lung_cancer_data
        extract_lung_cancer_data("cancer_2024.csv")
        logger.log(logging.INFO,"extract_air_pollution_data DONE!")
        
    @task
    def transform_load_dim_air_quality():
        from etl_scripts.transformation_scripts import air_quality_quantities_transformation
        from etl_scripts.loading_scripts import load_into_dim_air_qualities
        air_quality_df = air_quality_quantities_transformation()
        print(air_quality_df.shape)
        print(air_quality_df.head())
        load_into_dim_air_qualities(air_quality_df)
        logger.log(logging.INFO,"transform_load_dim_air_quality DONE!")
    
    @task
    def transform_and_load_dim_treatment_types():
        from etl_scripts.transformation_scripts import treatment_type_transformation
        from etl_scripts.loading_scripts import load_into_dim_treatment_types
        treatment_type_df = treatment_type_transformation()
        print(treatment_type_df.shape)
        load_into_dim_treatment_types(treatment_type_df)
        logger.log(logging.INFO,"transform_and_load_dim_treatment_types DONE!")
        

    @task
    def transform_and_load_dim_cancer_stages():
        from etl_scripts.transformation_scripts import cancer_stage_transformation
        from etl_scripts.loading_scripts import load_into_dim_cancer_stages
        cancer_stage_df = cancer_stage_transformation()
        print(cancer_stage_df.shape)
        load_into_dim_cancer_stages(cancer_stage_df)
        logger.log(logging.INFO,"transform_and_load_dim_cancer_stages DONE!")
        
    
    @task
    def transform_and_load_dim_regions():
        from etl_scripts.transformation_scripts import regions_tranformation
        from etl_scripts.loading_scripts import load_into_dim_regions
        regions_df = regions_tranformation()
        print(regions_df.shape)
        load_into_dim_regions(regions_df)
        logger.log(logging.INFO,"transform_and_load_dim_regions DONE!")
        
    
    @task
    def transform_and_load_dim_dates():
        from etl_scripts.transformation_scripts import dates_transformation
        from etl_scripts.loading_scripts import load_into_dim_dates
        dates_df = dates_transformation()
        print(dates_df.shape)
        load_into_dim_dates(dates_df)
        logger.log(logging.INFO,"transform_and_load_dim_dates DONE!")
        
    
    @task
    def transform_and_load_dim_countries():
        from etl_scripts.transformation_scripts import country_transformation
        from etl_scripts.loading_scripts import load_into_dim_countries
        countries_df = country_transformation()
        print(countries_df.shape)
        load_into_dim_countries(countries_df)
        logger.log(logging.INFO,"transform_and_load_dim_countries DONE!")
    
    @task
    def transform_and_load_dim_smoking_status():
        from etl_scripts.transformation_scripts import smoking_status_transformation
        from etl_scripts.loading_scripts import load_into_dim_smoking_status
        smoking_df = smoking_status_transformation()
        print(smoking_df.shape)
        load_into_dim_smoking_status(smoking_df)
        logger.log(logging.INFO,"transform_and_load_dim_smoking_status DONE!")
        

    @task
    def transform_and_load_fact_table():
        from etl_scripts.transformation_scripts import fact_transformation
        from etl_scripts.loading_scripts import load_into_fact_table
        fact_df = fact_transformation()
        print(fact_df.shape)
        load_into_fact_table(fact_df)
        logger.log(logging.INFO,"transform_and_load_fact_table DONE!")
        

    
    extract_air_pollution() >> extract_country_region() >> extract_lung_cancer() >> transform_load_dim_air_quality() >> transform_and_load_dim_treatment_types() >> transform_and_load_dim_cancer_stages() >> transform_and_load_dim_regions() >> transform_and_load_dim_dates() >> transform_and_load_dim_countries() >> transform_and_load_dim_smoking_status() >> transform_and_load_fact_table()

pipeline()
    
        
        