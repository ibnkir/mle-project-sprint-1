# scripts/fit.py

import pandas as pd
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from category_encoders import CatBoostEncoder
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.linear_model import LinearRegression, Lasso, Ridge
import yaml
import os
import joblib


def fit_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    data = pd.read_csv('data/initial_data.csv')

    cat_features = data.select_dtypes(include=['int', 'object'])
    potential_binary_features = cat_features.nunique() == 2
    binary_cat_features = cat_features[potential_binary_features[potential_binary_features].index]
    other_cat_features = cat_features[potential_binary_features[~potential_binary_features].index]
    num_features = data.select_dtypes(['float']).drop(columns=params['target_col'])
   
    preprocessor = ColumnTransformer(
        [
            ('binary', OneHotEncoder(drop=params['one_hot_drop']), binary_cat_features.columns.tolist()),
            ('cat', CatBoostEncoder(), other_cat_features.columns.tolist()),
            ('num', StandardScaler(), num_features.columns.tolist())
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )
  
    #model = LinearRegression(n_jobs=params['n_jobs'])
    model = Lasso(max_iter=params['max_iter'])
    
    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )
    pipeline.fit(data[data.columns.drop(params['target_col'])], data[params['target_col']])
 
    os.makedirs('models', exist_ok=True)
    with open('models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(pipeline, fd) 


if __name__ == '__main__':
    fit_model()