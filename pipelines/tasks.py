from prefect import task
import pandas as pd
import requests
import traceback
import datetime
import psycopg2
import os


API_URL = 'https://dados.mobilidade.rio/gps/brt'
CSV_PATH = 'data/brt_vehicles.csv'


@task
def get_data():
    response = requests.get(API_URL)
    data = response.json()
    return data['veiculos']


@task
def process_data(data):
    df = pd.DataFrame.from_records(data)
    df.replace({'': pd.NA, ' ': pd.NA}, inplace=True)
    df['dataHora'] = df['dataHora'].apply(lambda h: datetime.datetime.fromtimestamp(h/1000))

    return df


@task
def generate_csv(new_data): 
    try:
        df = pd.read_csv(CSV_PATH)
        concat_df = pd.concat([df, new_data])
        concat_df.drop_duplicates(inplace=True, keep=False)
        concat_df.to_csv(CSV_PATH, index=False)
        return concat_df
    except FileNotFoundError:
        new_data.to_csv(CSV_PATH, index=False)
        return new_data

@task
def store_data(df):
    conn = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        port=os.environ['POSTGRES_PORT'],
        database=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD']
    )

    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            CREATE TABLE brt_data_pgsql (
                id SERIAL PRIMARY KEY,
                codigo VARCHAR(20),
                placa VARCHAR(10),
                linha INTEGER,
                latitude numeric,
                longitude numeric,
                dataHora TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                velocidade numeric,
                id_migracao_trajeto VARCHAR(10),
                sentido VARCHAR(10),
                trajeto VARCHAR(100),
                hodometro numeric,
                direcao INTEGER
            )
            """
        )

        conn.commit()

        cursor.execute(
            """
            ALTER TABLE brt_data_pgsql
            ADD CONSTRAINT idx_unique
            UNIQUE (codigo, placa, linha, latitude, longitude, dataHora, velocidade, id_migracao_trajeto, sentido, trajeto, hodometro, direcao)
            """
        )
        
        conn.commit()
    
    except:
        pass

    query = """
        INSERT INTO brt_data_pgsql (codigo, placa, linha, latitude, longitude, dataHora, velocidade, id_migracao_trajeto, sentido, trajeto, hodometro, direcao)
        VALUES (%(codigo)s, %(placa)s, %(linha)s, %(latitude)s, %(longitude)s, %(dataHora)s, %(velocidade)s, %(id_migracao_trajeto)s, %(sentido)s, %(trajeto)s, %(hodometro)s, %(direcao)s)
        ON CONFLICT DO NOTHING
        """

    df = df.where(pd.notna(df), None)

    for _, row in df.iterrows():
        cursor.execute(query, row.to_dict())


    conn.commit()
    cursor.close()
    conn.close()
