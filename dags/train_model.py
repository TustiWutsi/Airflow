import warnings
warnings.filterwarnings("ignore")
import os

import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from joblib import dump


def compute_model_score(model, X, y):
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()

    return model_score


def train_and_save_model(model, X, y, path_to_model='./app/model.pckl'):
    # training the model
    model.fit(X, y)
    # saving model
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)


def prepare_data(path_to_data='/app/clean_data/fulldata.csv'):
    # reading data
    df = pd.read_csv(path_to_data)

    # ordering data according to city and date
    df = df.sort_values(['city', 'date'], ascending=True)

    dfs = []

    for c in df['city'].unique():
        df_temp = df[df['city'] == c]

        # creating target
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

        # creating features
        for i in range(1, 10):
            df_temp.loc[:, 'temp_m-{}'.format(i)
                        ] = df_temp['temperature'].shift(-i)

        # deleting null values
        df_temp = df_temp.dropna()

        dfs.append(df_temp)

    # concatenating datasets
    df_final = pd.concat(
        dfs,
        axis=0,
        ignore_index=False
    )

    # deleting date variable
    df_final = df_final.drop(['date'], axis=1)

    # creating dummies for city variable
    df_final = pd.get_dummies(df_final)

    features = df_final.drop(['target'], axis=1)
    target = df_final['target']

    return features, target

def prepare_data_dict(path_to_data):
    X, y = prepare_data(path_to_data)
    return X.to_dict(), y.to_dict()

def compute_model_score_dict(model, X_dict, y_dict):
    X = pd.DataFrame(X_dict)
    y = pd.DataFrame(y_dict)
    return compute_model_score(model, X, y)


if __name__ == '__main__':

    #X, y = prepare_data('./clean_data/fulldata.csv')
    X_dict, y_dict = prepare_data_dict('./clean_data/fulldata.csv')

    #score_lr = compute_model_score(LinearRegression(), X, y)
    #score_dt = compute_model_score(DecisionTreeRegressor(), X, y)
    score_lr = compute_model_score_dict(LinearRegression(), X_dict, y_dict)
    score_dt = compute_model_score_dict(DecisionTreeRegressor(), X_dict, y_dict)

    # using neg_mean_square_error
    if score_lr < score_dt:
        train_and_save_model(
            LinearRegression(),
            X,
            y,
            './clean_data/best_model.pickle'
        )
    else:
        train_and_save_model(
            DecisionTreeRegressor(),
            X,
            y,
            './clean_data/best_model.pickle'
        )