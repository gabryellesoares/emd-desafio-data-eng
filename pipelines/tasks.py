import os
import pandas as pd
import requests
import datetime
import psycopg2
from typing import Any
from prefect import task
from psycopg2 import extras
from pipelines.utils import log

API_URL = 'https://dados.mobilidade.rio/gps/brt'
CSV_PATH = 'data/brt_vehicles.csv'


@task
def get_data() -> str:
    """
    Gets data from an API

    Returns:
        str: all the data from the API_URL
    """
    response = requests.get(API_URL)
    data = response.json()

    log('Dados extraídos!')

    return data['veiculos']


@task
def process_data(data: Any) -> pd.DataFrame:
    """
    Converts JSON data to dataframe

    Args:
        data: JSON data

    Returns:
        pd.DataFrame
    """
    df_brt = pd.DataFrame.from_records(data)
    df_brt.replace({'': pd.NA, ' ': pd.NA}, inplace=True)
    df_brt['dataHora'] = df_brt['dataHora'].apply(
        lambda h: datetime.datetime.fromtimestamp(h/1000))

    log('DataFrame criado.')

    return df_brt


@task
def generate_csv(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Generates a .csv file without duplicates

    Args:
        dataframe: original dataframe

    Returns:
        pd.DataFrame: dataframe without duplicates
    """
    try:
        df = pd.read_csv(CSV_PATH)
        concat_df = pd.concat([df, dataframe])
        concat_df.drop_duplicates(inplace=True)
        concat_df.to_csv(CSV_PATH, index=False)
        log('Dados salvos no arquivo CSV.')
        return concat_df

    except FileNotFoundError:
        dataframe.to_csv(CSV_PATH, index=False)
        log('Arquivo CSV criado.')
        return dataframe


@task
def store_data(df):
    """
    Loads data into a PostgreSQL table

    Args:
        df: BRT data 
    """
    conn = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        port=os.environ['POSTGRES_PORT'],
        database=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD']
    )

    with conn.cursor() as cursor:
        create_query = """
            CREATE TABLE IF NOT EXISTS brt_data (
                codigo VARCHAR PRIMARY KEY,
                placa VARCHAR(10),
                linha VARCHAR,
                latitude NUMERIC,
                longitude NUMERIC,
                dataHora TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                velocidade NUMERIC,
                id_migracao_trajeto VARCHAR,
                sentido VARCHAR(10),
                trajeto VARCHAR,
                hodometro NUMERIC,
                direcao INTEGER
            )
            """

        cursor.execute(create_query)

        insert_query = """INSERT INTO brt_data VALUES %s ON CONFLICT DO NOTHING"""
        extras.execute_values(cursor, insert_query, df.values)

    conn.commit()
    conn.close()

    log('Dados carregados no PostgreSQL.')
