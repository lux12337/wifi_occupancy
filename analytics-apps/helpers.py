import pandas as pd

def getData(filename):
    data = pd.read_csv(filename)
    return data

# Converts time format (string) to a number(float) in 24 hour format
def time_to_int(str):
	t = str.split('-')[2].split(' ')[1].split(':')[0]
	return float(t)

# Takes in wifi_data's column (list) and a name/keyword of a building (string) to find its accesspoints
def get_building_accesspoints(lis, bui):
	ret = []
	for i in lis:
		if bui in i:
			ret.append(i)
	return ret    

	