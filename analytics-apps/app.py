#%matplotlib notebook
import matplotlib.pyplot as plt
import pandas as pd
import helpers as hp

data = hp.getData("wifi_data_until_20190204.csv")

# gca stands for 'get current axis'
ax = plt.gca()

data.plot(kind='line',x='time',y='POM-SUMNER212-AP205-2',ax=ax)
#data.plot(kind='line',x='time',y='POM-SMITH-AP275-4', color='red', ax=ax)
data.plot(kind='line',x='time',y='POM-WALTON-AP135-29', color='black', ax=ax)

plt.show()
