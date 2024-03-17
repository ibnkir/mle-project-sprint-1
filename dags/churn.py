# dags/churn.py

import pendulum
from airflow.decorators import dag, task
from steps.messages import send_telegram_success_message, send_telegram_failure_message


@dag(
    schedule='@once',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def prepare_churn_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    @task()
    def create_table():
        import sqlalchemy
        from sqlalchemy import MetaData, Table, Column, Integer, Boolean, BigInteger, Float, UniqueConstraint, inspect

        metadata = MetaData()
        table = Table(
            'flats_churn',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('flat_id', Integer),
            Column('floor', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('is_apartment', Boolean),
            Column('studio', Boolean),
            Column('total_area', Float),
            Column('price', BigInteger),
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Boolean),
            Column('target', Float),
            UniqueConstraint('flat_id', name='unique_flat_id_constraint_1')
        )

        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()
        if not inspect(engine).has_table(table.name):
            metadata.create_all(engine)
    
    @task()
    def extract():
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
        select
            f.id, f.floor, f.kitchen_area, f.living_area, f.rooms, f.is_apartment, f.studio, f.total_area, f.price,
            b.build_year, b.building_type_int, b.latitude, b.longitude, b.ceiling_height, b.flats_count, b.floors_total, b.has_elevator
        from flats as f
        left join buildings as b on f.building_id = b.id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        # Переименовываем колонку id у квартир на flat_id (т.к. id - это индексная колонка в БД)
        data.rename(columns={'id': 'flat_id'}, inplace=True)
        
        # Удаляем строки с пустыми, отрицательными и нулевыми ценами
        data = data[~(data['price'].isnull() | (data['price'] <= 0))]
        
        # Дабавляем целевой признак target = ln(1 + price)
        data['target'] = np.log1p(data['price'])

        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="flats_churn",
            replace=True,
            replace_index=['flat_id'],
            target_fields=data.columns.tolist(),
            rows=data.values.tolist()
        )
    
            
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
    
prepare_churn_dataset()
