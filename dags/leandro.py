from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from sqlalchemy import create_engine

from senhas_aws import aws_access_key_id, aws_access_key_secret, aws_postgres_password, aws_postgres_admin

import pandas as pd
import pymongo
import json
import boto3
import requests

#define default arguments:

default_args = {
    'owner': 'L. Bueno',
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["airflow@airflow.com.br"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),

#definindo as funções python utilizadas nas tasks:

def _extrai_dados_mongo():
    mongo = pymongo.MongoClient("mongodb+srv://estudante_igti:SRwkJTDz2nA28ME9@unicluster.ixhvw.mongodb.net/ibge?retryWrites=true&w=majority")
    db = mongo.ibge
    pnad_collect = db.pnadc20203
    df = pd.DataFrame(list(pnad_collect.find()))
    df.to_csv('/tmp/mongo.csv', index=False, encoding = 'utf-8', sep= ';')


def _extrai_dados_ibge():
    res = requests.get("https://servicodados.ibge.gov.br/api/v1/localidades/microrregioes")
    resjson = json.loads(res.text)
    df_ibge = pd.DataFrame(resjson)

    df_meso = pd.DataFrame(list(df_ibge['mesorregiao']))
    df_uf = pd.DataFrame(list(df_meso['UF']))
    df_regiao = pd.DataFrame(list(df_uf['regiao']))

    df_ibge['mesorregiao'] = df_meso['nome']
    df_ibge['UF'] = df_uf['sigla']
    df_ibge['regiao'] = df_regiao['nome']

    df_ibge.columns = ['id_microrregiao', 'microrregiao', 'mesorregiao', 'UF', 'regiao']
    df_ibge.to_csv('/tmp/ibge.csv', index=False, encoding = 'utf-8', sep= ';')

def _data_to_s3(filename):
    s3_client = boto3.client('s3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key = aws_access_key_secret)
    
    s3_client.upload_file(filename,"datalake-114855355396",filename[5:])

def _data_to_rds(filename):
    conn = create_engine(
        f'postgresql://{aws_postgres_admin}:{aws_postgres_password}@dev-rds-postgress-db.cm7petu2chju.us-east-2.rds.amazonaws.com:5432/onboarding_airflow')
    
    df = pd.read_csv(filename, sep = ';')
    df['dt'] = datetime.today()
    
    if filename == "/tmp/mongo.csv":
        df = df.loc[df['sexo'] == 'Mulher']
        df = df.loc[(df['idade'] >= 20) & (df['idade'] <= 40)]
    
    df.to_sql(filename[5:], conn, index = False, if_exists = 'replace', 
          method ='multi', chunksize = 1000)

#definindo a dag:

with DAG ("_extract_import_",
            description="pega os dados da API IBGE e do Mongo do cliente, insere arquivos no DW e s3"
            default_args=default_args,
            start_date=datetime(2021,1,1),
            schedule_interval=None,
            catchup=False) as dag:
        
        #Definindo as tasks:

        extrai_dados_mongo = PythonOperator(
            task_id="extrai_dados_mongo",
            python_callable =_extrai_dados_mongo
        )

        extrai_dados_ibge = PythonOperator(
            task_id="extrai_dados_ibge",
            python_callable=_extrai_dados_ibge
        )

        data_to_rds_mongo = PythonOperator(
            task_id='data_to_rds_mongo',
            python_callable= _data_to_rds,
            op_kwargs = {'filename':'/tmp/mongo.csv'},
        )

        data_to_rds_ibge = PythonOperator(
            task_id='data_to_rds_ibge',
            python_callable= _data_to_rds,
            op_kwargs = {'filename':'/tmp/ibge.csv'},
        )
        
        data_to_s3_ibge = PythonOperator(
            task_id='data_to_s3_ibge',
            python_callable= _data_to_s3,
            op_kwargs = {'filename':'/tmp/ibge.csv'},
            dag=dag
        )

        data_to_s3_mongo = PythonOperator(
            task_id='data_to_s3_mongo',
            python_callable= _data_to_s3,
            op_kwargs = {'filename':'/tmp/mongo.csv'},
        )

#Definindo as operações das task

extrai_dados_mongo >> [data_to_s3_mongo, data_to_rds_mongo]
extrai_dados_ibge  >> [data_to_s3_ibge, data_to_rds_ibge]