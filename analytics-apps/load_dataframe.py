# Create the DataFrame

# parameters for this cell
filepath: str = './../wifi_data_until_20190204.csv'
time_col_name = 'time'
time_col_index: int = 0
occupancy_type = 'int32'
occupancy_na = -1

data: pd.DataFrame = pd.read_csv(
    filepath_or_buffer=filepath,
    # time column
    index_col=time_col_index,
    parse_dates=[time_col_index],
    infer_datetime_format=True
).fillna(occupancy_na)

# All columns of the DataFrame should be of an unsigned integer type
for name, series in data.iteritems():
    # Occupancy count can only be nonnegative integers.
    # Occupancy in a building will never exceed 2^32.
    if name != time_col_name:
        data[name] = series.astype(dtype=occupancy_type, copy=False)
