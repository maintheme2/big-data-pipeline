import pandas as pd

data = pd.read_csv('data/accidents.csv')

data.drop(columns=['End_Lat', 'End_Lng', 'Precipitation(in)', 'Wind_Chill(F)'], inplace=True)

data.rename(columns={'Start_Lat': 'lat', 'Start_Lng': 'lng', 'Distance(mi)': 'Distance',
                     'Temperature(F)': 'Temperature', 'Humidity(%)': 'Humidity',
                     'Pressure(in)': 'Pressure', 'Visibility(mi)': 'Visibility',
                     'Wind_Speed(mph)': 'Wind_speed'}, inplace=True)

data.to_csv('data/accidents.csv', index=False)
