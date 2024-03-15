# dags/clean_churn.py

import pendulum
from airflow.decorators import dag, task
from steps.clean import remove_duplicates, fill_missing_values, remove_outliers
from steps.messages import send_telegram_success_message, send_telegram_failure_message


@dag(
    schedule='@once',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def clean_churn_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    @task()
    def create_table():
        import sqlalchemy
        from sqlalchemy import MetaData, Table, Column, Integer, BigInteger, Float, UniqueConstraint, inspect

        metadata = MetaData()
        table = Table(
            'clean_flats_churn',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('flat_id', Integer),
            Column('floor', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('is_apartment', Integer),
            Column('studio', Integer),
            Column('total_area', Float),
            Column('price', BigInteger),
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Integer),
            Column('target', Float),
            UniqueConstraint('flat_id', name='unique_clean_flat_id_constraint')
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
        select * from flats_churn
        """
        data = pd.read_sql(sql, conn).drop(columns=['id'])
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        data = remove_duplicates(data)
        data = fill_missing_values(data)
        data = remove_outliers(data)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="clean_flats_churn",
            replace=True,
            replace_index=['flat_id'],
            target_fields=data.columns.tolist(),
            rows=data.values.tolist()
        )
    
                    
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
    
clean_churn_dataset()
