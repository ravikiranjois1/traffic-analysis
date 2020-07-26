__author__ = "Abhay Rajendra Dixit "
__author__ = "Pranjal Pandey"
__author__ = "Ravikiran Jois Yedur Prabhakar"


import json
import sys
import time
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np
from pymongo import MongoClient
from matplotlib import pyplot as plot


def get_mongo_params(file):
    with open(file, 'r') as f:
        source = json.loads(f.read())
        return dict(
            host=source['host'],
            port=source['port'],
            database=source['database']
        )


def get_files_path_params(file):
    with open(file, 'r') as f:
        source = json.loads(f.read())

        return dict(
            traffic_crash_data_path = str(source['traffic_crash_data_path']),
            redlight_data_path = str(source['redlight_data_path']),
            speed_violations_data_path = str(source['speed_violations_data_path']),
        )


def get_mongo_connection(mongo_dict):
    try:
        connection = MongoClient("mongodb://" + mongo_dict['host'] + ":" + str(mongo_dict['port']))
        print("Connected to MongoDb successfully!")
    except:
        print("Could not connect to MongoDB")
    return mongo_dict["database"], connection


def read_data():
    red_light_df = pd.read_csv(filename_dict["redlight_data_path"], usecols=["ADDRESS", "VIOLATION DATE", "VIOLATIONS"])

    crash_df = pd.read_csv(filename_dict["traffic_crash_data_path"],
                           usecols=["CRASH_DATE", "STREET_NO", "STREET_NAME", "STREET_DIRECTION", "POSTED_SPEED_LIMIT",
                                    "FIRST_CRASH_TYPE", "TRAFFICWAY_TYPE", "PRIM_CONTRIBUTORY_CAUSE"])

    speed_df = pd.read_csv(filename_dict["speed_violations_data_path"], usecols=["ADDRESS", "VIOLATION DATE", "VIOLATIONS"])

    crash_df[['Date', 'Time', 'M']] = crash_df.CRASH_DATE.str.split(" ", expand=True, )

    return red_light_df, crash_df, speed_df


def process_red_light_data(red_light_frame):
    print("Processing red light violations dataset...")
    red_light_frame["ADDRESS"].replace(to_replace=[" roa$| ROA$", " ave$| AVE$", " stree$| STREE$",
                                                   " boulev$| BOULEV$", " dr$| DR$", " parkwa$| PARKWA$", " st$| ST$"],
                                       value=[" ROAD", " AVENUE", " STREET", " BOULEVARD", " DRIVE", " PARKWAY", " STREET"],
                                       regex=True, inplace=True)

    red_light_frame["STREET_NO_DIR"] = red_light_frame["ADDRESS"].str.split(' ', 2)
    red_light_frame["STREET_NAME"] = red_light_frame["ADDRESS"].str.split().str[2:]
    red_light_frame["STREET_NAME"] = [' '.join(map(str, l)) for l in red_light_frame['STREET_NAME']]
    red_light_frame[['STREET_NO', 'STREET_DIR', 'OTHERS']] = pd.DataFrame(red_light_frame.STREET_NO_DIR.tolist(),
                                                                          index=red_light_frame.index)
    del red_light_frame['OTHERS']


def process_speed_data(speed_sample):
    print("Processing speed camera violations dataset...")
    speed_sample["ADDRESS"].replace(to_replace=[" rd$| RD$", " av$| AV$| ave$| AVE$", " st$| stree$| STREE$| ST$",
                                                " blvd$| BLVD$", " dr$| DR$", " parkwa$| PARKWA$", " hwy$| HWY$"],
                                    value=[" ROAD", " AVENUE", " STREET", " BOULEVARD", " DRIVE", " PARKWAY",
                                           " HIGHWAY"],
                                    regex=True, inplace=True)

    speed_sample["STREET_NO_DIR"] = speed_sample["ADDRESS"].str.split(' ', 2)
    speed_sample["STREET_NAME"] = speed_sample["ADDRESS"].str.split().str[2:]
    speed_sample["STREET_NAME"] = [' '.join(map(str, l)) for l in speed_sample['STREET_NAME']]
    speed_sample[['STREET_NO', 'STREET_DIR', 'OTHERS']] = pd.DataFrame(speed_sample.STREET_NO_DIR.tolist(),
                                                                       index=speed_sample.index)

    del speed_sample['OTHERS']


def process_crash_data(traffic_frame):
    print("Processing traffic crashes dataset...")
    traffic_frame["STREET_NAME"].replace(to_replace=[" rd$| RD$", " ave$| AVE$| av$| AV$", " st$| ST$",
                                                     " blvd$| BLVD$", " dr$| DR$", " pkwy$| PKWY$"],
                                         value=[" ROAD", " AVENUE", " STREET", " BOULEVARD", " DRIVE", " PARKWAY"],
                                         regex=True, inplace=True)

    traffic_frame['STREET_NAME'].replace(np.nan, -9999, inplace=True)


def process_data(traffic_frame, red_light_frame, speed_sample):

    # For Red Light Violations
    process_red_light_data(red_light_frame)

    # For Traffic Crashes
    process_crash_data(traffic_frame)

    # For Speed Camera Violations
    process_speed_data(speed_sample)

    clean_red_light_frame, clean_traffic_frame, clean_speed_frame = select_attributes(red_light_frame, traffic_frame, speed_sample)
    return clean_red_light_frame, clean_traffic_frame, clean_speed_frame


def select_attributes(red_light_frame, traffic_frame, speed_frame):
    traffic_frame.drop(["CRASH_DATE", "Time", "M"], axis=1, inplace=True)
    red_light_frame.drop(["STREET_NO_DIR", "ADDRESS"], axis=1, inplace=True)
    speed_frame.drop(["STREET_NO_DIR", "ADDRESS"], axis=1, inplace=True)
    return red_light_frame, traffic_frame, speed_frame


def insert_data_to_mongo(traffic_analysis, mongo_con, traffic_frame, red_light_frame, speed_frame):
    db = mongo_con[traffic_analysis]
    traffic_crash_collection = db['traffic_crash']
    violation_collection = db['violation']
    speed_camera_collection = db['speed']

    traffic_frame["Date"].replace({" ": ""}, inplace=True)
    traffic_frame.reset_index(inplace=True)
    traffic_frame_dict = traffic_frame.to_dict("records")

    print("Inserting traffic data to MongoDB")
    traffic_crash_collection.insert_many(traffic_frame_dict)

    print("Insert Red Light Data to traffic collection")
    red_light_frame["VIOLATION DATE"].replace({" ": ""}, inplace=True)
    red_light_frame_dict = red_light_frame.to_dict("records")
    violation_collection.insert_many(red_light_frame_dict)

    print("Insert Speed Camera Data to traffic collection")
    speed_frame["VIOLATION DATE"].replace({" ": ""}, inplace=True)
    speed_frame_dict = speed_frame.to_dict("records")
    speed_camera_collection.insert_many(speed_frame_dict)


def get_stats(red_light_frame, speed_frame, traffic_crash):
    red_light_stats = red_light_frame.VIOLATIONS.describe()
    speed_camera_stats = speed_frame.VIOLATIONS.describe()
    traffic_crash = traffic_crash.POSTED_SPEED_LIMIT.describe()
    print("Descriptive statistics for the number of Red Light Violations")
    print(red_light_stats)
    print()
    print("Descriptive statistics for the number of Speed Camera Violations")
    print(speed_camera_stats)
    print()
    print("Descriptive statistics for the number of Posted Speed Limit")
    print(traffic_crash)


if __name__ == '__main__':
    start_time = time.time()

    config_path = sys.argv[1]
    mongo_connection_file = config_path + "/connection.json"
    data_file_path = config_path + "/path.json"

    global filename_dict

    filename_dict = get_files_path_params(data_file_path)

    mongo_dict = get_mongo_params(mongo_connection_file)
    db_name, mongo_conn = get_mongo_connection(mongo_dict)

    red_light_frame, traffic_frame, speed_frame = read_data()
    red_light_frame, traffic_frame, speed_frame = process_data(traffic_frame, red_light_frame, speed_frame)

    insert_data_to_mongo(db_name, mongo_conn, traffic_frame, red_light_frame, speed_frame)
    print()
    print("--------------- Data Inserted to MongoDB ----------------")
    print()
    get_stats(red_light_frame, speed_frame, traffic_frame)
    print()
    print("Total time taken for the process: ", time.time()-start_time)
