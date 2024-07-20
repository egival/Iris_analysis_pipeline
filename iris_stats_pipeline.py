from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pandas as pd

def transforming_csv_file_data():
    """ 
    Removes null values
    """
    df = pd.read_csv('/home/egidija1/airflow/datasets/iris.csv')
    df_result = df.dropna()
    print(df_result)
    df_result.to_csv('/home/egidija1/airflow/output/no_null.csv', index=False)

def brief_stats():
    """ 
    Calculates brief stats of all species of iris flower in the dataset
    """
    df = pd.read_csv('/home/egidija1/airflow/output/no_null.csv')
    stats_df = df.agg({
        'sepal_length': ['count', 'mean', 'min', 'max'],
        'sepal_width': ['count', 'mean', 'min', 'max'],
        'petal_length': ['count', 'mean', 'min', 'max'],
        'petal_width': ['count', 'mean', 'min', 'max'],
    }).reset_index()
    print(stats_df)
    stats_df.to_csv('/home/egidija1/airflow/output/brief_stats.csv', index=False)
    
def group_by_species():
    """
    Grouping data by species and calculating stats
    """
    df = pd.read_csv('/home/egidija1/airflow/output/no_null.csv')
    species_df = df.groupby('species').agg({
        'sepal_length': ['count', 'mean', 'min', 'max'],
        'sepal_width': ['count', 'mean', 'min', 'max'],
        'petal_length': ['count', 'mean', 'min', 'max'],
        'petal_width': ['count', 'mean', 'min', 'max'],
    }).reset_index()
    print(species_df)
    species_df.to_csv('/home/egidija1/airflow/output/grouped_by_species.csv', index=False)

with DAG(
    'Iris_stats_pipeline',
    start_date=datetime(2024, 7, 1),
    description='Runing a Python pipeline to get brief stats of flower Iris',
    tags=['python', 'transform', 'pipeline'],
    schedule='@once', 
    catchup=False,
    default_args={
        'owner':'egi'
    }
):

    task1 = PythonOperator(
        task_id = 'transforming_csv_file_data',
        python_callable = transforming_csv_file_data
    )

    task2 = PythonOperator(
        task_id = 'brief_stats',
        python_callable= brief_stats
    )

    task3 = PythonOperator(
        task_id = 'group_by_species',
        python_callable= group_by_species
    )

task1 >> [task2, task3]