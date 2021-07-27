from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pymongo


def _extrai_dados_mongo():
    return print("olá leandro do mongo")

def _extrai_dados_ibge():
    return print("olá leandro do ibge")

    return


with DAG ("minha_dagzinha", start_date=datetime(2021,1,1),
            schedule_interval="@daily", catchup=False) as dag:
        
        extrai_dados_mongo = PythonOperator(
            task_id="extrai_dados_mongo"
            python_callabable=_extrai_dados
        )

        extrai_dados_ibge = PythonOperator(
            task_id="extrai_dados_ibge"
            python_callabable=_extrai_dados
        )


extrai_dados_ibge >> extrai_dados_mongo
