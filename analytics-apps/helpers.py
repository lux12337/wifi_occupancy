import pandas as pd

def getData(filename):
    data = pd.read_csv(filename)
    return data

    
