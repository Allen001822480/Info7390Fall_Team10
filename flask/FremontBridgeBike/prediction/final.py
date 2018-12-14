import forecastio
import datetime
import pandas as pd
import getpass
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import date
import holidays
import pickle
sns.set(style="ticks", color_codes=True)
#api = getpass.getpass()
api = "3427f6e6a362d849da3380b2d8a17890"
# Seattle location
lat = 47.606209
lng = -122.332069


def get_input(days):
    days = 1
    attributes = ["temperature", "humidity", "pressure", "windBearing", "windSpeed"]
    times = []
    data = {}

    for attr in attributes:
        data[attr] = []

    current = datetime.datetime.now()
    for offset in range(days - 1, days + 1):
        forecast = forecastio.load_forecast(api, lat, lng, time=current + datetime.timedelta(offset), units='si')
        h = forecast.hourly()
        d = h.data
        for p in d:
            times.append(p.time)
            for attr in attributes:
                data[attr].append(p.d[attr])
    df = pd.DataFrame(data, index=times)
    df.index.names = ['datetime']
    df = df.reset_index()
    future = current + datetime.timedelta(days)
    #len = df.shape[0]
    df["date"] = [d.date() for d in df["datetime"]]
    df = df[df['date'] == future.date()]

    # add new colunms Week Status and Day_Of_Week and Hour
    weekStatus = np.empty(24)
    dayOfWeek = np.empty(24)
    day_week = []
    hour = np.empty(24)
    for i in list(range(0, 24)):
        dayOfWeek[i] = df.iloc[i]['datetime'].isoweekday()
        if dayOfWeek[i] < 6:
            weekStatus[i] = 1
        else:
            weekStatus[i] = 0
        hour[i] = df.iloc[i]['datetime'].hour

        if dayOfWeek[i] == 1:
            day_week.append('Mon')
        if dayOfWeek[i] == 2:
            day_week.append('Tue')
        if dayOfWeek[i] == 3:
            day_week.append('Wed')
        if dayOfWeek[i] == 4:
            day_week.append('Thu')
        if dayOfWeek[i] == 5:
            day_week.append('Fri')
        if dayOfWeek[i] == 6:
            day_week.append('Sat')
        if dayOfWeek[i] == 7:
            day_week.append('Sun')
    for i in list(range(0, 24)):
        if df.iloc[i]['datetime'].date() in holidays.US():
            weekStatus[i] = 0
    season = []
    for i in list(range(0, 24)):
        if df.iloc[i]['datetime'].month in [1, 2, 3, 4, 12]:
            season.append('Winter')
        if df.iloc[i]['datetime'].month in [5, 6]:
            season.append('Spring')
        if df.iloc[i]['datetime'].month in [7, 8, 9]:
            season.append('Summer')
        if df.iloc[i]['datetime'].month in [10, 11]:
            season.append('Fall')
    df = df.assign(season=season)
    #result['season'] = result['season'].astype('category')
    # make Week_Status,Hour and Day_of_Week as categorical data
    df = df.assign(Work_Status=weekStatus)

    df = df.assign(Day_of_Week=day_week)
    #df['Day_of_Week'] = df['Day_of_Week'].astype('category')
    #df['Day_of_Week'].cat.set_categories = ["Mon", "Tue", "Wed", "Thu", "Fri","Sat","Sun"]
    df = df.assign(Hour=hour)
    df['Hour'] = df['Hour'].astype('category')
    # return the ideal dataframe
    result = pd.DataFrame()
    result['hour'] = df['Hour']
    result['temperature'] = df['temperature']
    result['humidity'] = df['humidity'] * 100
    result['pressure'] = df['pressure']
    result['wind_direction'] = df['windBearing']
    result['wind_speed'] = df['windSpeed']
    result['work_status'] = df['Work_Status']
    result['day_of_week'] = df['Day_of_Week']
    result['season'] = df['season']
    #result = pd.get_dummies(result)
    return result

# get_input(2)


def feature_engineering(df_in):
    sample = pd.read_csv('prediction/sample.csv', low_memory=False)
    df = df_in.copy()
    df_com = sample.append(df)
    df_com = pd.get_dummies(data=df_com, columns=['day_of_week', 'hour', 'season'])
    return df_com.tail(24)

# feature_engineering(get_input(2))


# def read_from_pickle(path):
#     with open(path, 'rb') as file:
#         try:
#             while True:
#                 yield pickle.load(file)
#         except EOFError:
#             pass


def east_prediction(df_x):
    filename = 'gaoshunan_east_model.sav'
    east_model = pickle.load(open(filename, 'rb'))
    east_model = pickle.load(open(filename, 'rb'))
    result = east_model.predict(df_x)
    hour = np.arange(0, 24, 1)
    plt.plot(hour, result)
    plt.xlabel('Time (h)')
    plt.ylabel('East side bike flow')
    plt.title('East Prediction')
    plt.grid(True)
    plt.savefig("static/p-east.png")
    # plt.show()


def west_prediction(df_x):
    filename = 'gaoshunan_west_model.sav'
    west_model = pickle.load(open(filename, 'rb'))
    result = west_model.predict(df_x)
    hour = np.arange(0, 24, 1)
    plt.plot(hour, result)
    plt.xlabel('Time (h)')
    plt.ylabel('West side bike flow')
    plt.title('West Prediction')
    plt.grid(True)
    plt.savefig("static/p-west.png")
    # plt.show()


def getPrediction(day):
    df_input = get_input(day)
    df_X = feature_engineering(df_input)
    east_prediction(df_X)
    west_prediction(df_X)


'''
attributes = ["temperature","humidity","pressure","windBearing","windSpeed"]
times = []
data = {}
for attr in attributes:
    data[attr] = []

start = datetime.datetime(2018,12,11)
for offset in range(-1,1):
    forecast = forecastio.load_forecast(api,lat,lng,time = start+datetime.timedelta(offset),units='si')
    h = forecast.hourly()
    d = h.data
    for p in d:
        times.append(p.time)
        for attr in attributes:
            data[attr].append(p.d[attr])
df = pd.DataFrame(data,index=times)
df.index.names = ['datetime']
df = df.reset_index()
print(df)
'''


# if __name__ == '__main__':
#     get_input(2)
