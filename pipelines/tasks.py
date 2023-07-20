from prefect import task
import pandas as pd
import requests
import datetime

API_URL = 'https://dados.mobilidade.rio/gps/brt'

@task
def get_data():
    response = requests.get(API_URL)
    data = response.json()
    return data['veiculos']


@task
def transform_data(vehicles):
    transf_data = []

    for v in vehicles:
        transf_data.append([v['codigo'], v['latitude'], v['longitude'], v['velocidade']])
    
    return transf_data

@task
def generate_csv(data): # usando o transformed_data
    df = pd.DataFrame(data, columns=['id', 'latitude', 'longitude', 'speed'])
    filename = f'brt_data_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}'
    df.to_csv(filename, index=False)
    return filename


@task
def generate_csv(data): # todos os dados
    df = pd.DataFrame.from_records(data)
    df.replace({'': pd.NA, ' ': pd.NA}, inplace=True)
    filename = f'brt_data_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}'
    df.to_csv(filename, index=False)
    return filename