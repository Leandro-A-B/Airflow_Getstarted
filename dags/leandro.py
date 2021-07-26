from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd


def _extrai_dados():
    return


with DAG ("minha_dagzinha", start_date=datetime(2021,1,1),
            schedule_interval="@daily", catchup=False) as dag:
        
        atividade1 = PythonOperator(
            task_id="extrai_dados_mongo"
            python_callabable=_extrai_dados
        )

        atividade2 = PythonOperator(
            task_id="extrai_dados_"
            python_callabable=_extrai_dados
        )