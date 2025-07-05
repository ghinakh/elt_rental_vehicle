# type: ignore

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery, storage
import os
from dotenv import load_dotenv
import shutil
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator 
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.bash import BashOperator 
from airflow.sdk import Variable

# Load .env
load_dotenv()

# Read MySQL connection info
hostname = os.getenv("MYSQL_HOST")
port = int(os.getenv("MYSQL_PORT")) 
username = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")

TEMP_DIR = "/tmp/rent_vhc_data"

# GCP info
bucket_name = os.getenv("BUCKET_NAME")
project_id = os.getenv("PROJECT_ID")
gcp_service_account = os.getenv("SERVICE_ACCOUNT")

yesterday_str = (datetime.today().date() - timedelta(days=1)).strftime('%Y-%m-%d')
start_date = Variable.get("rent_vhc_start_date")


if not os.path.exists(gcp_service_account): 
    raise FileNotFoundError(f"Service account key not found at: {gcp_service_account}")
else:
    print(f"[INFO] Service account key found at: {gcp_service_account}")

# DAG
with DAG(
    'elt_rent_vhc_dag',
    start_date=datetime(2025, 7, 4),
    schedule='@daily',
    catchup=False,
    tags=['portofolio']
) as dag:
    
    # Task1: import users, vehicles, transactions from mysql to gcs
    @task
    def mysql_to_gcs():
        # connect to mysql
        engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{hostname}:{port}/rental_vehicle")

        # select users, vehicles, transactions where date >= 2024-01-01 and date < today() -> 2025-07-04 
        # (it should return data from 2024-01-01 until 2025-07-03 or the original data)
        df_users = pd.read_sql(f"SELECT * FROM users WHERE creation_date >= '{start_date}' AND creation_date < CURDATE();", con=engine)
        df_vehicles = pd.read_sql(f"SELECT * FROM vehicles WHERE STR_TO_DATE(last_update_timestamp, '%d-%m-%Y') >= '{start_date}' AND STR_TO_DATE(last_update_timestamp, '%d-%m-%Y') < CURDATE();", con=engine)
        df_transactions = pd.read_sql(f"SELECT * FROM transactions WHERE DATE(rental_end_time) >= '{start_date}' AND DATE(rental_end_time) < CURDATE();", con=engine)


        # save it locally in container
        os.makedirs(TEMP_DIR, exist_ok=True)
        df_users.to_parquet(f"{TEMP_DIR}/users_cur_{yesterday_str}.parquet", index=False)
        df_vehicles.to_parquet(f"{TEMP_DIR}/vehicles_cur_{yesterday_str}.parquet", index=False)
        df_transactions.to_parquet(f"{TEMP_DIR}/transactions_cur_{yesterday_str}.parquet", index=False)

        # save to GCS
        client = storage.Client.from_service_account_json(gcp_service_account)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f'rent_vhc/mysql_export/users_cur_{yesterday_str}.parquet')
        blob.upload_from_filename(f"{TEMP_DIR}/users_cur_{yesterday_str}.parquet")
        blob = bucket.blob(f'rent_vhc/mysql_export/vehicles_cur_{yesterday_str}.parquet')
        blob.upload_from_filename(f"{TEMP_DIR}/vehicles_cur_{yesterday_str}.parquet")
        blob = bucket.blob(f'rent_vhc/mysql_export/transactions_cur_{yesterday_str}.parquet')
        blob.upload_from_filename(f"{TEMP_DIR}/transactions_cur_{yesterday_str}.parquet")

    #Task2: Clean TEMP_DIR
    @task
    def cleanup_temp_dir():
        if os.path.exists(TEMP_DIR):
            shutil.rmtree(TEMP_DIR)
            print(f"TEMP_DIR at {TEMP_DIR} has been cleaned up.")
        else:
            print(f"TEMP_DIR {TEMP_DIR} not found, nothing to clean.")

    # Task Pre-3
    create_staging_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_staging_dataset_if_not_exists',
        dataset_id='staging_dataset',
        project_id=project_id,
        gcp_conn_id='google_cloud_default',
        exists_ok=True  # biar tidak error kalau dataset-nya SUDAH ADA
    )

    # Task3-1: GCS to BQ Staging (raw data users)
    gcs_to_bq_users = GCSToBigQueryOperator(
        task_id='gcs_to_bq_users',
        bucket=bucket_name,
        source_objects=[f'rent_vhc/mysql_export/users_cur_{yesterday_str}.parquet'],
        destination_project_dataset_table=f'{project_id}.staging_dataset.raw_users',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )

    # Task3-2: GCS to BQ Staging
    gcs_to_bq_vehicles = GCSToBigQueryOperator(
        task_id='gcs_to_bq_vehicles',
        bucket=bucket_name,
        source_objects=[f'rent_vhc/mysql_export/vehicles_cur_{yesterday_str}.parquet'],
        destination_project_dataset_table=f'{project_id}.staging_dataset.raw_vehicles',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )

    # Task3-3: GCS to BQ Staging
    gcs_to_bq_transactions = GCSToBigQueryOperator(
        task_id='gcs_to_bq_transactions',
        bucket=bucket_name,
        source_objects=[f'rent_vhc/mysql_export/transactions_cur_{yesterday_str}.parquet'],
        destination_project_dataset_table=f'{project_id}.staging_dataset.raw_transactions',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )

    # Task4: dbt seed
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command="""
            export DBT_ALLOW_FOREIGN_PATHS=true &&
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt clean &&
            dbt deps --profiles-dir . &&
            dbt seed --profiles-dir .
        """
    )
    
    # Task5-1: clean location_enrichment
    dbt_run_clean_location = BashOperator(
        task_id='dbt_run_clean_location',
        bash_command="""
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt run --select staging.location_enrichment --profiles-dir . 
        """
    )

    # Task5-2: clean users
    dbt_run_clean_users = BashOperator(
        task_id='dbt_run_clean_users',
        bash_command="""
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt run --select staging.clean_users --profiles-dir . 
        """
    )

    # Task5-3: clean vehicles
    dbt_run_clean_vehicles = BashOperator(
        task_id='dbt_run_clean_vehicles',
        bash_command="""
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt run --select staging.casted_vehicles --profiles-dir . &&
            dbt run --select staging.clean_vehicles --profiles-dir . 
        """
    )

    # Task5-4: clean transaction
    dbt_run_clean_transactions = BashOperator(
        task_id='dbt_run_clean_transactions',
        bash_command="""
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt run --select staging.clean_transactions --profiles-dir . 
        """
    )

    # Task6-1: Snapshot users
    dbt_snap_users = BashOperator(
        task_id='dbt_snap_users',
        bash_command="""
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt snapshot --select snapshot_users --profiles-dir . 
        """
    )

    # Task6-2: Snapshot vehicles
    dbt_snap_vehicles = BashOperator(
        task_id='dbt_snap_vehicles',
        bash_command="""
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt snapshot --select snapshot_vehicles --profiles-dir . 
        """
    )
    
    # Task7-1: Reporting core (dim_users)
    dbt_run_dimusers = BashOperator(
        task_id='dbt_run_dimusers',
        bash_command="""
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt run --select reporting.dim_users --profiles-dir . 
        """
    )

    # Task7-2: Reporting core (dim_vehicles)
    dbt_run_dimvehicles = BashOperator(
        task_id='dbt_run_dimvehicles',
        bash_command="""
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt run --select reporting.dim_vehicles --profiles-dir . 
        """
    )

    # Task7-3: Reporting core (dim_locations)
    dbt_run_dimlocs = BashOperator(
        task_id='dbt_run_dimlocs',
        bash_command="""
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt run --select reporting.dim_locations --profiles-dir . 
        """
    )

    # Task7-4: Reporting core (fact_transactions)
    dbt_run_facttrans = BashOperator(
        task_id='dbt_run_facttrans',
        bash_command="""
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt run --select reporting.fact_transactions --profiles-dir . 
        """
    )

    # Task8: Join
    dbt_run_denorm = BashOperator(
        task_id='dbt_run_denorm',
        bash_command="""
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt run --select transform.denormalized_transactions --profiles-dir . 
        """
    )

    # Task9: users_summary
    dbt_run_users_summ = BashOperator(
        task_id='dbt_run_users_summ',
        bash_command="""
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt run --select reporting.users_summary --profiles-dir . 
        """
    )

    # Task9: vehicles_summary
    dbt_run_vehicles_summ = BashOperator(
        task_id='dbt_run_vehicles_summ',
        bash_command="""
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt run --select reporting.vehicles_summary --profiles-dir . 
        """
    )

    # Task9: daily_performance
    dbt_run_daily_perf = BashOperator(
        task_id='dbt_run_daily_perf',
        bash_command="""
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt run --select reporting.daily_performance --profiles-dir . 
        """
    )

    # Task9: daily_top_entities
    dbt_run_daily_top_ent = BashOperator(
        task_id='dbt_run_daily_top_ent',
        bash_command="""
            cd /usr/local/airflow/include/dbt_rent_vhc &&
            dbt run --select reporting.daily_top_entities --profiles-dir . 
        """
    )

    @task
    def update_start_date():
        for_tomorrow = datetime.now().strftime("%Y-%m-%d")
        Variable.set("rent_vhc_start_date", for_tomorrow)
    
    
    # set dependencies
    mysql_to_gcs() >> cleanup_temp_dir() >> create_staging_dataset
    create_staging_dataset >> [gcs_to_bq_users, gcs_to_bq_vehicles, gcs_to_bq_transactions] >> dbt_seed
    dbt_seed >> dbt_run_clean_location >> dbt_run_dimlocs
    dbt_run_dimlocs >> [dbt_run_clean_users, dbt_run_clean_transactions, dbt_run_clean_vehicles]
    dbt_run_clean_users >> dbt_snap_users >> dbt_run_dimusers
    dbt_run_clean_vehicles >> dbt_snap_vehicles >> dbt_run_dimvehicles
    [dbt_run_dimlocs, dbt_run_dimusers, dbt_run_dimvehicles, dbt_run_clean_transactions] >> dbt_run_facttrans
    dbt_run_facttrans >> dbt_run_denorm
    dbt_run_denorm >> [dbt_run_users_summ, dbt_run_vehicles_summ]
    dbt_run_vehicles_summ >> dbt_run_daily_perf >> dbt_run_daily_top_ent
    dbt_run_daily_top_ent >> update_start_date()
