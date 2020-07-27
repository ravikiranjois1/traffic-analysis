from matplotlib import lines

__author__ = "Abhay Rajendra Dixit "
__author__ = "Pranjal Pandey"
__author__ = "Ravikiran Jois Yedur Prabhakar"

import datetime
import json
import sys
import time
import pandas as pd
import matplotlib.pyplot as plt
import copy

pd.options.mode.chained_assignment = None  # default='warn'
from pymongo import MongoClient
import matplotlib.lines
import pingouin as pg
import numpy as np
import json


def get_mongo_params(file):
    """
         This function reads the files and initialise mongoDB configuration
         for the project
         :param file: file path of the config file
         :return filePath dictionary: The dictionary of connection information like port, host and database.
    """
    with open(file, 'r') as f:
        source = json.loads(f.read())
        return dict(
            host=source['host'],
            port=source['port'],
            database=source['database']
        )


def get_files_path_params(file):
    """
         This function reads file path of three data-sets from the config files
         :param file: file path of the config file
         :return filePath dictionary: The dictionary of all the source file paths
    """
    with open(file, 'r') as f:
        source = json.loads(f.read())

        return dict(
            traffic_crash_data_path=str(source['traffic_crash_data_path']),
            redlight_data_path=str(source['redlight_data_path']),
            speed_violations_data_path=str(source['speed_violations_data_path']),
        )


def get_mongo_connection(mongo_dict):
    """
         This function reads connection dictionary file and establish the
         connection to mongoDb database
         :param mongo_dict: The dictionary of all the source file paths
         :return collection: Mongodb connection string which contains host and port information
    """
    try:
        connection = MongoClient("mongodb://" + mongo_dict['host'] + ":" + str(mongo_dict['port']))
        print("Connected to MongoDb successfully!")
    except:
        print("Could not connect to MongoDB")
    return mongo_dict["database"], connection


def time_series_analysis_combined(traffic_analysis, mongo_con):
    """
        Method to display combined information of the time series data for each collection i.e., Speed Camera Violations,
        Traffic Camera violations and Red Light Violation
        :param traffic_analysis: the name of the database on mongoDB
        :param mongo_conn: mongoDB connection object
        :return: None
        """
    db = mongo_con[traffic_analysis]

    """With Speed"""
    speed_time = list(db.speed.aggregate([
        {'$project': {
            "VIOLATIONS": 1,
            "month": {"$month": "$VIOLATION DATE"}
        }},
        {'$group': {
                '_id': "$month",
                'total': {"$sum": "$VIOLATIONS"}
            }
        },
        {'$sort': {'_id': 1}}
    ]))
    print(speed_time)
    months = []
    violations = []
    for item in speed_time:
        months.append(item['_id'])
        violations.append(item['total'])

    plt.fill_between(months, violations, color="orange", label='Speed Camera')
    plt.xticks(months)
    # plt.show()

    """With Violations"""
    violation_coll = list(db.violation.aggregate([
        {'$project': {
            "VIOLATIONS": 1,
            "month": {"$month": "$VIOLATION DATE"}
        }},
        {'$group': {
            '_id': "$month",
            'total': {"$sum": "$VIOLATIONS"}
            }
        },
        {'$sort': {'_id': 1}}
    ]))
    print(violation_coll)
    months = []
    violations = []
    for item in violation_coll:
        months.append(item['_id'])
        violations.append(item['total'])

    plt.fill_between(months, violations, color='red', label='Red Light')
    plt.xticks(months)
    # plt.show()

    """With Traffic Violations"""
    traffic_crash = list(db.traffic_crash.aggregate([
        {'$project':
            {"month": {"$month": "$Date"}}
        },
        {'$group': {
            '_id': "$month",
            'total': {"$sum": 1}
            }
        },
        {'$sort': {'_id': 1}}
    ]))
    print(traffic_crash)
    months = []
    violations = []
    for item in traffic_crash:
        months.append(item['_id'])
        violations.append(item['total'])

    plt.fill_between(months, violations, color="skyblue", label='Traffic Crashes')
    plt.xticks(months)
    plt.legend(loc="upper right")
    plt.xlabel("Month")
    plt.ylabel("No. of Violations")
    plt.show()


def time_series_analysis_separated(traffic_analysis, mongo_conn):
    """
    Method to display the time series data for each collection i.e., Speed Camera Violations, Traffic Camera violations
    and Red Light Violation separately
    :param traffic_analysis: the name of the database on mongoDB
    :param mongo_conn: mongoDB connection object
    :return: None
    """
    month_long_to_short = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep",
                           10: "Oct", 11: "Nov", 12: "Dec"}
    year_long_to_short = {2015: '15', 2016: '16', 2017: '17', 2018: '18', 2019: '19', 2020: '20'}
    db = mongo_conn[traffic_analysis]

    """Speed Collection"""
    speed_data = list(db.speed.aggregate([
                        {'$group': {
                                '_id': {
                                    'year': {'$year': '$VIOLATION DATE'},
                                    'month': {'$month': '$VIOLATION DATE'}
                                },
                                'total': {'$sum': '$VIOLATIONS'}}
                        }, {'$sort': {'_id': 1}}
                    ]))
    months = []
    years = []
    violations = []
    for item in speed_data:
        months.append(item['_id']['month'])
        years.append(item['_id']['year'])
        violations.append(item['total'])

    speed = {}
    speed['Month'] = months
    speed['Year'] = years
    speed['Violations'] = violations

    speed_df = pd.DataFrame(speed)
    speed_df['Month'] = speed_df['Month'].map(month_long_to_short)
    speed_df['Year'] = speed_df['Year'].map(year_long_to_short)
    # speed_df['Year'] = speed_df['Year'].astype(str)
    speed_df['Month'] = speed_df['Month'].astype(str)
    speed_df['Month_Year'] = speed_df['Month'] + " '" + speed_df['Year']
    speed_df['Moving Averages'] = speed_df.rolling(window=6).mean()
    ax_speed = speed_df.set_index('Month_Year')['Violations'].plot(kind='line', figsize=(20, 10), color='purple', rot=90,
                                                                   label="Speed Camera Violations", grid=True)
    speed_df.set_index('Month_Year')['Moving Averages'].plot(kind='line', figsize=(20, 10), color='cadetblue',
                                                                   rot=90, label="Simple Moving Average (Violations)", grid=True)
    # speed_df.set_index('Month_Year')['Violations'].plot(kind='bar', figsize=(20, 10), color='cadetblue',
    #                                                                rot=90, position=0.1, label="Speed Camera Violations")
    ax_speed.set_xticks(speed_df.index)
    ax_speed.set_xticklabels(speed_df['Month_Year'], rotation=90)

    plt.legend(loc="upper right")
    plt.title("Speed Camera Violation vs. Month")
    plt.xlabel("Month")
    plt.ylabel("No. of Violations")
    plt.show()

    """Red Light Violations Collection"""
    red_light = list(db.violation.aggregate([
        {'$group': {
            '_id': {
                'year': {'$year': '$VIOLATION DATE'},
                'month': {'$month': '$VIOLATION DATE'}
            },
            'total': {'$sum': '$VIOLATIONS'}}
        }, {'$sort': {'_id': 1}}
    ]))
    months = []
    years = []
    violations = []
    for item in red_light:
        months.append(item['_id']['month'])
        years.append(item['_id']['year'])
        violations.append(item['total'])

    red_light_dict = {}
    red_light_dict['Month'] = months
    red_light_dict['Year'] = years
    red_light_dict['Violations'] = violations

    red_light_df = pd.DataFrame(red_light_dict)
    red_light_df['Month'] = red_light_df['Month'].map(month_long_to_short)
    red_light_df['Year'] = red_light_df['Year'].map(year_long_to_short)
    # red_light_df['Year'] = red_light_df['Year'].astype(str)
    red_light_df['Month'] = red_light_df['Month'].astype(str)
    red_light_df['Month_Year'] = red_light_df['Month'] + " '" + red_light_df['Year']
    red_light_df['Moving Averages'] = red_light_df.rolling(window=6).mean()
    ax_red_light = red_light_df.set_index('Month_Year')['Violations'].plot(kind='line', figsize=(20, 10), color='red',
                                                                           rot=90, label="Red Light Violations", grid=True)
    red_light_df.set_index('Month_Year')['Moving Averages'].plot(kind='line', figsize=(20, 10), color='black', rot=90,
                                                            label="Simple Moving Average (Violations)", grid=True)

    ax_red_light.set_xticks(speed_df.index)
    ax_red_light.set_xticklabels(speed_df['Month_Year'], rotation=90)

    plt.legend(loc="upper right")
    plt.title("Red Light Violation vs. Month")
    plt.xlabel("Month")
    plt.ylabel("No. of Violations")
    plt.show()

    """Traffic Crashes Collection"""
    traffic_crash = list(db.traffic_crash.aggregate([
        {'$group': {'_id': {
                        'year': {'$year': '$Date'},
                        'month': {'$month': '$Date'}
                      },
                      'total': {'$sum': 1}}
        }, {'$sort': {'_id': 1}}
    ]))
    months = []
    years = []
    violations = []
    for item in traffic_crash:
        months.append(item['_id']['month'])
        years.append(item['_id']['year'])
        violations.append(item['total'])

    traffic_dict = {}
    traffic_dict['Month'] = months
    traffic_dict['Year'] = years
    traffic_dict['Violations'] = violations

    traffic_crash_df = pd.DataFrame(traffic_dict)
    traffic_crash_df['Month'] = traffic_crash_df['Month'].map(month_long_to_short)
    traffic_crash_df['Year'] = traffic_crash_df['Year'].map(year_long_to_short)

    traffic_crash_df['Month'] = traffic_crash_df['Month'].astype(str)
    traffic_crash_df['Month_Year'] = traffic_crash_df['Month'] + " '" + traffic_crash_df['Year']
    traffic_crash_df['Moving Averages'] = traffic_crash_df.rolling(window=6).mean()
    ax_traffic_crash = traffic_crash_df.set_index('Month_Year')['Violations'].plot(kind='line', figsize=(20, 10), color='green',
                                                                           rot=90, label="Traffic Crashes", grid=True)
    traffic_crash_df.set_index('Month_Year')['Moving Averages'].plot(kind='line', figsize=(20, 10), color='orange', rot=90,
                                                            label="Simple Moving Average (Violations)", grid=True)

    ax_traffic_crash.set_xticks(speed_df.index)
    ax_traffic_crash.set_xticklabels(speed_df['Month_Year'], rotation=90)

    plt.legend(loc="upper right")
    plt.title("Traffic Crashes vs. Month")
    plt.xlabel("Month")
    plt.ylabel("No. of Violations")
    plt.show()


def time_series_analysis_red_deseasoning(traffic_analysis, mongo_conn):
    """
    Method to display the time series data for Red Light Violation separately
    :param traffic_analysis: the name of the database on mongoDB
    :param mongo_conn: mongoDB connection object
    :return: None
    """
    month_long_to_short = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep",
                           10: "Oct", 11: "Nov", 12: "Dec"}
    year_long_to_short = {2015: '15', 2016: '16', 2017: '17', 2018: '18', 2019: '19', 2020: '20'}
    db = mongo_conn[traffic_analysis]

    """Red Light Violations Collection"""
    red_light = list(db.violation.aggregate([
        {'$group': {
            '_id': {
                'year': {'$year': '$VIOLATION DATE'},
                'month': {'$month': '$VIOLATION DATE'}
            },
            'total': {'$sum': '$VIOLATIONS'}}
        }, {'$sort': {'_id': 1}}
    ]))

    months = []
    years = []
    violations = []
    for item in red_light:
        months.append(item['_id']['month'])
        years.append(item['_id']['year'])
        violations.append(item['total'])

    red_light_dict = {}
    red_light_dict['Month'] = months
    red_light_dict['Year'] = years
    red_light_dict['Violations'] = violations

    red_light_df = pd.DataFrame(red_light_dict)

    red_light_seasoner = red_light_df['Violations'].groupby([red_light_df.Year, red_light_df.Month], sort=False).sum().unstack()
    red_light_seasoner_original = copy.copy(red_light_seasoner)
    red_light_seasoner_mean_rows = red_light_seasoner.mean(axis=1)
    for index in range(2015, 2021):
        for col in red_light_seasoner:
            red_light_seasoner[col][index] = red_light_seasoner[col][index] / red_light_seasoner_mean_rows[index]

    red_light_seasoner_mean_cols = red_light_seasoner.mean(axis=0)
    index = 1
    for col in red_light_seasoner:
        # for index in range(len(red_light_seasoner)):
        red_light_seasoner[col] = red_light_seasoner_original[col] / red_light_seasoner_mean_cols[index]
        index += 1

    red_light_seasoner = red_light_seasoner.unstack().reset_index()
    red_light_seasoner.rename(columns={0: 'Violations'}, inplace=True)
    columns_titles = ["Year", "Month", "Violations"]
    red_light_seasoner = red_light_seasoner.reindex(columns=columns_titles)
    df_2015 = red_light_seasoner[red_light_seasoner['Year'] == 2015]
    df_2016 = red_light_seasoner[red_light_seasoner['Year'] == 2016]
    df_2017 = red_light_seasoner[red_light_seasoner['Year'] == 2017]
    df_2018 = red_light_seasoner[red_light_seasoner['Year'] == 2018]
    df_2019 = red_light_seasoner[red_light_seasoner['Year'] == 2019]
    df_2020 = red_light_seasoner[red_light_seasoner['Year'] == 2020]
    red_light_seasoner = pd.concat([df_2015, df_2016, df_2017, df_2018, df_2019, df_2020])

    red_light_df['Month'] = red_light_df['Month'].map(month_long_to_short)
    red_light_df['Year'] = red_light_df['Year'].map(year_long_to_short)
    # red_light_df['Year'] = red_light_df['Year'].astype(str)
    red_light_df['Month'] = red_light_df['Month'].astype(str)
    red_light_df['Month_Year'] = red_light_df['Month'] + " '" + red_light_df['Year']
    red_light_seasoner['Month'] = red_light_seasoner['Month'].map(month_long_to_short)
    red_light_seasoner['Year'] = red_light_seasoner['Year'].map(year_long_to_short)
    # red_light_df['Year'] = red_light_df['Year'].astype(str)
    red_light_seasoner['Month'] = red_light_seasoner['Month'].astype(str)
    red_light_seasoner['Month_Year'] = red_light_seasoner['Month'] + " '" + red_light_seasoner['Year']

    ax_red_light = red_light_df.set_index('Month_Year')['Violations'].plot(kind='line', figsize=(20, 10), color='red',
                                                                rot=90, label="Red Light Violations", grid=True)
    red_light_seasoner.set_index('Month_Year')['Violations'].plot(kind='line', figsize=(20, 10), color='black',
                                                                rot=90, label="Deseasoned Violations (Violations)", grid=True)

    ax_red_light.set_xticks(red_light_df.index)
    ax_red_light.set_xticklabels(red_light_df['Month_Year'], rotation=90)

    plt.legend(loc="upper right")
    plt.title("Red Light Violation vs. Month")
    plt.xlabel("Month")
    plt.ylabel("No. of Violations")
    plt.show()


def visualize(x_df, y_df, x_label, y_label, cor, color):
    """
    Method to display graphs depicting correlation between red light violations, speed violations and total violation
    against crashes with respect to date and location.
    :param x_df: dataframe as x-axis
    :param y_df: dataframe as y-axis
    :param x_label: label for x-axis
    :param y_label: label for y-axis
    :param cor: correlation coefficient to be displayed
    :param color: color of the curve
    :return: None
    """
    f1 = plt.figure()
    plt.scatter(x_df, y_df, label="Pearson's Correlation Coefficient = {:.3f}".format(cor.r.pearson), color=color, s=15, marker="o")
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(x_label + " v/s " + y_label)
    plt.legend()
    plt.draw()
    return f1


def date_perspective(speed_df, red_light_df, traffic_crash_df):
    """
    Method to calculate and display correlation between redlight violations, speed violations and total violation against crashes with
    respect to date.
    :param speed_df: dataframe consisting of speed violation data
    :param red_light_df: dataframe consisting of red light violation data
    :param traffic_crash_df: dataframe consisting of traffic crash data
    :return: None
    """


    date_red_light_frame = red_light_df[['VIOLATION DATE', 'VIOLATIONS']]
    date_speed_frame = speed_df[['VIOLATION DATE', 'VIOLATIONS']]
    date_traffic_crash = traffic_crash_df[["Date"]]
    date_red_light_frame = date_red_light_frame.groupby('VIOLATION DATE', sort=False, as_index=False)[
        'VIOLATIONS'].sum()
    date_speed_frame = date_speed_frame.groupby('VIOLATION DATE', sort=False, as_index=False)['VIOLATIONS'].sum()
    date_traffic_crash['count'] = date_traffic_crash.groupby('Date')['Date'].transform('count')
    date_traffic_crash.rename(columns={'count': 'Crashes'}, inplace=True)
    date_speed_frame.rename(columns={'VIOLATION DATE': 'Date', 'VIOLATIONS': 'Red_Light_Violations'}, inplace=True)
    date_red_light_frame.rename(columns={'VIOLATION DATE': 'Date', 'VIOLATIONS': 'Speed_Limit_Violations'},
                                inplace=True)

    res = pd.merge(date_traffic_crash, date_speed_frame, how='left', on='Date', sort=True).drop_duplicates()
    res = pd.merge(res, date_red_light_frame, how='left', on='Date', sort=True).drop_duplicates()
    res['Total_Violations'] = res['Red_Light_Violations'] + res['Speed_Limit_Violations']
    res.fillna(0, inplace=True)
    cor1 = pg.corr(x=res['Red_Light_Violations'], y=res['Crashes'])
    cor2 = pg.corr(x=res['Speed_Limit_Violations'], y=res['Crashes'])
    cor3 = pg.corr(x=res['Total_Violations'], y=res['Crashes'])

    f1 = visualize(res["Red_Light_Violations"], res["Crashes"], "Red Light Violations", "Crashes", cor1, 'blue')
    f2 = visualize(res["Speed_Limit_Violations"], res["Crashes"], "Speed-limit Violations", "Crashes", cor2, 'blue')
    f3 = visualize(res["Total_Violations"], res["Crashes"], "Total Violations", "Crashes", cor3, 'blue')

    return f1, f2, f3


def location_perspective(speed_df, red_light_df, traffic_crash_df):
    """
    Method to display correlation between red light violations, speed violations and total violation against crashes with
    respect to location.
    :param speed_df: dataframe consisting of speed violation data
    :param red_light_df: dataframe consisting of red light violation data
    :param traffic_crash_df: dataframe consisting of traffic crash data
    :return: None
    """
    speed_frame_sample = speed_df[['STREET_NAME', 'VIOLATIONS']].groupby('STREET_NAME', as_index=False).sum()
    red_light_frame_sample = red_light_df[['STREET_NAME', 'VIOLATIONS']].groupby('STREET_NAME', as_index=False).sum()
    traffic_frame_sample = traffic_crash_df[['STREET_NAME', 'Date']].groupby('STREET_NAME', as_index=False).count()

    red_light_frame_sample.columns = ["STREET_NAME", "REDLIGHT_VIOLATIONS"]
    speed_frame_sample.columns = ["STREET_NAME", "SPEED_VIOLATIONS"]
    res = pd.merge(traffic_frame_sample, speed_frame_sample, how='left', on='STREET_NAME')
    res = pd.merge(res, red_light_frame_sample, how='left', on='STREET_NAME')
    res.columns = ["STREET_NAME", "REDLIGHT_VIOLATIONS", "SPEED_VIOLATIONS", "Crashes"]
    res["Total_violations"] = res["REDLIGHT_VIOLATIONS"] + res["SPEED_VIOLATIONS"]
    res['REDLIGHT_VIOLATIONS'].fillna(0, inplace=True)
    res['SPEED_VIOLATIONS'].fillna(0, inplace=True)
    res['Crashes'].fillna(0, inplace=True)
    res['Total_violations'].fillna(0, inplace=True)

    cor1 = pg.corr(x=res['REDLIGHT_VIOLATIONS'], y=res['Crashes'])
    cor2 = pg.corr(x=res['SPEED_VIOLATIONS'], y=res['Crashes'])
    cor3 = pg.corr(x=res['Total_violations'], y=res['Crashes'])

    f1 = visualize(res["REDLIGHT_VIOLATIONS"], res["Crashes"], "Red Light Violations", "Crashes", cor1, 'red')
    f2 = visualize(res["SPEED_VIOLATIONS"], res["Crashes"], "Speed Camera Violations", "Crashes", cor2, 'orange')
    f3 = visualize(res["SPEED_VIOLATIONS"], res["Crashes"], "Total Violations", "Crashes", cor3, 'lightblue')

    return f1, f2, f3


def correlation():
    db = mongo_conn["traffic_analysis"]
    speed_df = pd.DataFrame(list(db.speed.find({})))
    red_light_df = pd.DataFrame(list(db.violation.find({})))
    traffic_crash_df = pd.DataFrame(list(db.traffic_crash.find({})))

    date_perspective(speed_df, red_light_df, traffic_crash_df)

    location_perspective(speed_df, red_light_df, traffic_crash_df)


if __name__ == '__main__':
    """
        Main function:
        Print the output,call the functions, prints
        the overall time taken.
    """
    start_time = time.time()

    config_path = sys.argv[1]
    mongo_connection_file = config_path + "/connection.json"
    data_file_path = config_path + "/path.json"

    global filename_dict

    filename_dict = get_files_path_params(data_file_path)

    mongo_dict = get_mongo_params(mongo_connection_file)
    db_name, mongo_conn = get_mongo_connection(mongo_dict)

    db = mongo_conn[db_name]

    # time_series_analysis_combined(db_name, mongo_conn)
    time_series_analysis_separated(db_name, mongo_conn)
    time_series_analysis_red_deseasoning(db_name, mongo_conn)
    correlation()

    plt.show()
