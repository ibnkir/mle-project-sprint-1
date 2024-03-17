# scripts/data.py

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import yaml
from datetime import datetime


def create_connection():
    load_dotenv()
    host = os.environ.get('DB_DESTINATION_HOST')
    port = os.environ.get('DB_DESTINATION_PORT')
    db = os.environ.get('DB_DESTINATION_NAME')
    username = os.environ.get('DB_DESTINATION_USER')
    password = os.environ.get('DB_DESTINATION_PASSWORD')
    
    print(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    conn = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}', connect_args={'sslmode':'require'})
    return conn


def get_data():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    conn = create_connection()
    data = pd.read_sql('select * from clean_flats_churn', conn, index_col=params['index_col'])
    
    # Вместо года постройки добавляем возраст здания
    data['building_age'] = (datetime.now().year - data['build_year']).astype('float')

    # Удаляем лишние колонки
    data.drop(
        columns=['id', 'build_year', 'longitude', 'latitude', 'is_apartment', 'studio', 'price'], 
        inplace=True
    )
    
    # Изменяем тип булевских столбцов и building_type_int на object
    bool_cols = data.select_dtypes('bool').columns
    data[bool_cols] = data[bool_cols].astype('object')
    data['building_type_int'] = data['building_type_int'].astype('object')

    # Изменяем тип количественных целых признаков на float
    num_int_cols = data.select_dtypes('int').columns
    data[num_int_cols] = data[num_int_cols].astype('float')
    
    os.makedirs('data', exist_ok=True)
    data.to_csv('data/initial_data.csv', index=None)
    conn.dispose()


if __name__ == '__main__':
    get_data()
