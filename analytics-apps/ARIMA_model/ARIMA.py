#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# ARIMA used for short term forecast

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns; sns.set(style="whitegrid")
import pandas as pd

from statsmodels.tsa.arima_model import ARIMA
from pandas.plotting import autocorrelation_plot
from datetime import datetime, timedelta

from dataframe_manip import get_building_accesspoints


# In[ ]:


# FUNCTIONS

def hour_rounder(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +timedelta(hours=t.minute//30))

def prepare_byHour_df_for_ARIMA(name, data):
    """
    Takes a name of a building and finds the
    total average of devices in the building
    at the time. Returns a pd.Series
    
    Note: 1st column is the time (doesn't change
    from original table). 2nd column is total
    devices at the moment
    """
    building_ap = get_building_accesspoints(data.columns, name)
    ret_df = pd.DataFrame(columns = ['time','total'])
    
    ret_df['time'] = pd.to_datetime(
        data['time'].apply(hour_rounder),
        infer_datetime_format=True,
        utc=True
    )
    
    ret_df['total'] = data[building_ap].fillna(0).sum(axis=1)

    ret_df = ret_df.set_index('time')
    
    # !!!!!!! Must exclusively assign !!!!!!!!!!
    # FInd groupby by one hour, assign to var, use resample
    # Predict for the far future & predict for near furture
    #   Which is the near future
    # State clearly what are the conditions
    ret_df = ret_df.groupby(['time']).mean() 
    
    ret_df.index.freq = ret_df.index.inferred_freq
    
    return ret_df

def split_train_test_average_to_hour(data, percent):
    """
    Takes a dataframe with a DatetimeIndex and averages
    and splits them depending on the given percent.
    The percent defines the length of train set.
    Returns tuple of train then test
    """
    train_size = int(len(data) * percent)
    train_ret, test_ret = data[0:train_size],data[train_size:len(data)]

    #train_ret = train.groupby(train.index.hour).mean()
    #test_ret = test.groupby(test.index.hour).mean()
    
    return train_ret,test_ret


# In[ ]:


# invert differenced value
def inverse_difference(history, yhat, interval=1):
    return yhat + history[-interval]

def rm_extra_array(arr):
    ret = []
    for i in arr:
        ret.append(i[0])
    return ret

def forecast_oneStep_outOfSample_ARIMA(df,p_arima,d_arima,q_arima):
    model = ARIMA(df,order=(p_arima,d_arima,q_arima))
    model_fit = model.fit(disp=0)
    forecast = model_fit.forecast()[0]
    return forecast[0]
    
def forecast_multiStep_outOfSample_ARIMA(df,step,p_arima,d_arima,q_arima):
    model = ARIMA(df,order=(p_arima,d_arima,q_arima))
    model_fit = model.fit(disp=0)
    forecast = model_fit.forecast(steps=step)[0]
    
    history = rm_extra_array([x for x in df.values])
    
    #print(forecast)
    ret = []
    for y in forecast:
        #print("y: "+str(y))
        i = inverse_difference(history, y, 24)
        history.append(i)
        ret.append(i)

    return ret


# In[ ]:


# POSSIBLE INPUT
filename = 'wifi_data_until_20190204.csv'
building_name = 'CLARK'
p_arima = 5
d_arima = 1
q_arima = 0


# In[ ]:


# Get data from csv  file; Try not to modify it!!!
wifi_df = pd.read_csv(
    filename,
    infer_datetime_format=True,
    header=0,
    parse_dates=[0],
    #squeeze=True,
    index_col=False
)


# In[ ]:


# Gets a Series object that represents number of devices
# connected based on time every hour
building_time_df = prepare_byHour_df_for_ARIMA(
    building_name, wifi_df)


# In[ ]:


# Separated training set from testing set
# Only getting one month because its a hella a lot of data
train_hour_df, test_hour_df = split_train_test_average_to_hour(building_time_df, 0.66)


# In[ ]:


forecast_oneStep_outOfSample_ARIMA(train_hour_df,p_arima,d_arima,q_arima)


# In[ ]:


forecast_multiStep_outOfSample_ARIMA(train_hour_df,10,p_arima,d_arima,q_arima)

