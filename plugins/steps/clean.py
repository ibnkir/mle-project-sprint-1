# plugins/steps/clean.py

import pandas as pd
import numpy as np


def remove_duplicates(data):
    cols_to_check = data.columns.drop(['flat_id']).tolist()
    duplicated_rows = data.duplicated(subset=cols_to_check, keep=False)
    data = data[~duplicated_rows].reset_index(drop=True)
    return data


def fill_missing_values(data):
    cols_with_nans = data.isnull().sum()
    cols_with_nans = cols_with_nans[cols_with_nans > 0].index
    
    # 1. В колонке flat_id не может быть пропусков, т.к. она была индексом в исходной таблице flats
    # 2. В цикл не попадают колонки price и target, т.к. ранее мы уже удалили строки с пустыми ценами
    for col in cols_with_nans:
        if data[col].dtype in ['float']:
            fill_value = data[col].mean()
        elif data[col].dtype in ['int', 'bool', 'object']:
            fill_value = data[col].mode().iloc[0]
        
        data[col].fillna(value=fill_value, inplace=True)
    
    return data


def remove_outliers(data):
    num_cols = data.select_dtypes(['int', 'float']).drop(
        columns=['flat_id', 'building_type_int', 'price', 'target']
    ).columns
    threshold = 1.5
    potential_outliers = pd.DataFrame()
    
    for col in num_cols:
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1
        margin = threshold * IQR
        lower = Q1 - margin
        upper = Q3 + margin
        potential_outliers[col] = ~data[col].between(lower, upper)
        
    outliers = potential_outliers.any(axis=1)
    return data[~outliers]
