from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import RandomizedSearchCV
from sklearn.model_selection import KFold
from sklearn.metrics import mean_squared_error, mean_absolute_error
import pandas as pd
import matplotlib.pyplot as plt
from math import *

def calc_adj_r2(r2, n, k):
    """ Calculate and return adjusted r2 score.
    Parameters
    ----------
    r2  :
        Original r2 score.
    n   :
        Number of points in data sample.
    k   :
        Number of variables in model, excluding the constant.
    Returns
    -------
    float
        Adjusted R2 score.
    """
    return 1 - (((1 - r2) * (n - 1)) / (n - k - 1))

def add_time_features(data, year=False, month=False, week=False, tod=False, dow=False):
    """ Add time features to dataframe.
    Parameters
    ----------
    year    : bool
        Year.
    month   : bool
        Month.
    week    : bool
        Week.
    tod    : bool
        Time of Day.
    dow    : bool
        Day of Week.
    """

    var_to_expand = []

    if year:
        data["year"] = data.index.year
        var_to_expand.append("year")
    if month:
        data["month"] = data.index.month
        var_to_expand.append("month")
    if week:
        data["week"] = data.index.week
        var_to_expand.append("week")
    if tod:
        data["tod"] = data.index.hour
        var_to_expand.append("tod")
    if dow:
        data["dow"] = data.index.weekday
        var_to_expand.append("dow")

    # One-hot encode the time features
    for var in var_to_expand:

            add_var = pd.get_dummies(data[var], prefix=var, drop_first=True)

            # Add all the columns to the model data
            data = data.join(add_var)

            # Drop the original column that was expanded
            data.drop(columns=[var], inplace=True)
            
    return data

def train_model(df, st_training_period, et_training_period, with_occ_data=False):
    
    if not with_occ_data:
        remove_col = ['occupancy', 'power']
    else:
        remove_col = ['power']
    
    baseline_in = df.loc[st_training_period:et_training_period, [col for col in df.columns if col not in remove_col]]
    baseline_out = df.loc[st_training_period:et_training_period, ['power']]

    # Out-of-box linear regression
    model = LinearRegression()
    scores = []

    kfold = KFold(n_splits=3, shuffle=True, random_state=42)
    for i, (train, test) in enumerate(kfold.split(baseline_in, baseline_out)):
        model.fit(baseline_in.iloc[train], baseline_out.iloc[train])
        scores.append(model.score(baseline_in.iloc[test], baseline_out.iloc[test]))

    mean_score = sum(scores) / len(scores)
    adj_r2 = calc_adj_r2(mean_score, baseline_in.shape[0], baseline_out.shape[1])

    print('R2: ', mean_score)
    print('Adj R2: ', adj_r2)
    
    return model

def make_predictions(model, df, testing_period, with_occ_data=False):
    
    if not with_occ_data:
        remove_col = ['occupancy', 'power']
    else:
        remove_col = ['power']
        
    test_df = df.loc[testing_period, [col for col in df.columns if col not in remove_col]]
    test_df = test_df.interpolate()

    project_df = pd.DataFrame()
    project_df = df.loc[testing_period, ['power']]

    project_df['y_pred'] = model.predict(test_df)
    project_df.columns = ['true_power', 'predicted_power']
    
    return project_df

def plot_data(project_df, testing_period, bldg_name, with_occ_data=False):
    
    title = 'Demand Prediction ' + bldg_name + ' (without occupancy data)' if not with_occ_data else 'Demand Prediction ' + bldg_name + ' (with occupancy data)'
    #filename = 'without_occupancy.png' if not with_occ_data else 'with_occupancy.png'
    
    ax = project_df.plot(figsize=(10,7), title=title)
    ax.set_xlabel(testing_period)
    ax.set_ylabel('Power (kW)')

    print('RMSE: ', sqrt(mean_squared_error(project_df.iloc[:,0], project_df.iloc[:,1])))
    print('MAE: ', mean_absolute_error(project_df.iloc[:,0], project_df.iloc[:,1]))