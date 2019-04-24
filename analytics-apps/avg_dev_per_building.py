#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns; sns.set(style="whitegrid")
import pandas as pd

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from dataframe_manip import get_building_accesspoints,csv_to_dataframe


# In[2]:


# Get data from csv  file; Try not to modify it!!!
wifi_df = pd.read_csv('wifi_data_until_20190204.csv',
                      infer_datetime_format=True,
                      index_col=False)


# In[3]:


# Get list of APs for multiple buildings
#building_list = ['ALEXANDER','MASON','THATCHER','SCC','CLARK','PEARSON','SMITH']

# Get list of APs for a buildings
building_name = 'MASON'
building_ap = get_building_accesspoints(wifi_df.columns, building_name)

# Create a Dataframe to help shape the final dataframe
building_df = pd.DataFrame(columns = ['month','day','hour','total']+building_ap)


# In[4]:


# Populate building_df
building_df['month'] = pd.to_datetime(
    wifi_df['time'],
    infer_datetime_format=True,
    utc=True
).dt.strftime('%b')#.astype(np.float) #%m

building_df['day'] = pd.to_datetime(
    wifi_df['time'],
    infer_datetime_format=True,
    utc=True
).dt.strftime('%a')#.astype(np.float) #%w

building_df['hour'] = pd.to_datetime(
    wifi_df['time'],
    infer_datetime_format=True,
    utc=True
).dt.strftime('%H').astype(np.float)

building_df[building_ap] = wifi_df[building_ap].fillna(0)

building_df['total'] = building_df[building_ap].sum(axis=1)


# In[5]:


# Apply Multiple indexes in datafram
building_df_by_all = building_df.groupby(['month','day','hour']).mean()
#print(list(building_df_by_day_and_hour['total']))


# In[8]:


# Creates an empty DataFrame foor scatterplot
final_df = pd.DataFrame(
    columns = ['month','day','hour','building_name','average_total']
)

final_df['month'] = building_df_by_all.index.get_level_values(0)
final_df['day'] = building_df_by_all.index.get_level_values(1)
final_df['hour'] = building_df_by_all.index.get_level_values(2)
final_df['building_name'] = [building_name]*len(final_df['month'])
final_df['total'] = list(building_df_by_all['total'])

#print(final_df)


# In[9]:


# Lists to help order plots
order_day = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
order_mon = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep","Oct","Nov","Dec"]

# Create 1x3 subplots
fig, (ax1, ax2, ax3) = plt.subplots(1,3,figsize=(20,8))

ax2.set_title(building_name,fontsize=50)

sns.barplot(x="day", y="total", data=final_df, ax=ax1, order=order_day)

sns.barplot(x="hour", y="total", data=final_df, ax=ax2)
plt.setp(ax2.get_xticklabels(), rotation=45)
ax2.set_ylabel("")  # remove y label, but keep ticks

sns.barplot(x="month", y="total", data=final_df, ax=ax3, order=order_mon)
ax3.set_ylabel("")  # remove y label, but keep ticks

fig.tight_layout()

plt.show()


# In[ ]:




