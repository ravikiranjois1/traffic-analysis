import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np
from pymongo import MongoClient
from matplotlib import pyplot as plot


def read_data():
    traffic_crashes = "/Users/ravikiranjois/Documents/RIT/Semester 3 - Summer '20/Data Cleaning and Preparation/Project/Datasets/Traffic_Crashes.csv"
    red_light_violations = "/Users/ravikiranjois/Documents/RIT/Semester 3 - Summer '20/Data Cleaning and Preparation/Project/Datasets/Red_Light_Camera_Violations.csv"
    speed_camera = "/Users/ravikiranjois/Documents/RIT/Semester 3 - Summer '20/Data Cleaning and Preparation/Project/Datasets/Speed_Camera_Violations.csv"

    red_light_df = pd.read_csv(red_light_violations, usecols=["ADDRESS", "VIOLATION DATE", "VIOLATIONS"])

    crash_df = pd.read_csv(traffic_crashes,
                           usecols=["CRASH_DATE", "STREET_NO", "STREET_NAME", "STREET_DIRECTION", "POSTED_SPEED_LIMIT",
                                    "FIRST_CRASH_TYPE", "TRAFFICWAY_TYPE", "PRIM_CONTRIBUTORY_CAUSE"])

    speed_df = pd.read_csv(speed_camera, usecols=["ADDRESS", "VIOLATION DATE", "VIOLATIONS"])

    red_light_sample = pd.DataFrame(red_light_df.ADDRESS.str.split(' ', 2).tolist(),
                                    columns=['STREET_NO', 'STREET_DIRECTION', 'STREET_NAME'])

    speed_sample = pd.DataFrame(speed_df.ADDRESS.str.split(' ', 2).tolist(),
                                    columns=['STREET_NO', 'STREET_DIRECTION', 'STREET_NAME'])

    crash_df[['Date', 'Time', 'M']] = crash_df.CRASH_DATE.str.split(" ", expand=True, )

    red_light_df_sample = red_light_df.head(10000)
    crash_df_sample = crash_df.head(10000)
    speed_sample = speed_df.head(10000)

    red_light_df_sample.to_csv("/Users/ravikiranjois/Documents/RIT/Semester 3 - Summer '20/Data Cleaning and Preparation/Project/Datasets/Red_Light_Camera_Violations_sample.csv",
        sep='\t', encoding='utf-8')
    crash_df_sample.to_csv("/Users/ravikiranjois/Documents/RIT/Semester 3 - Summer '20/Data Cleaning and Preparation/Project/Datasets/Traffic_Crashes_sample.csv",
        sep='\t', encoding='utf-8')
    speed_sample.to_csv("/Users/ravikiranjois/Documents/RIT/Semester 3 - Summer '20/Data Cleaning and Preparation/Project/Datasets/Speed_Camera_Sample.csv",
        sep='\t', encoding='utf-8')

    return red_light_df_sample, crash_df_sample, speed_sample


def process_data(traffic_frame, red_light_frame, speed_sample):

    # For Red Light Violations
    red_light_frame["ADDRESS"].replace(to_replace=[" roa$| ROA$", " ave$| AVE$", " stree$| STREE$",
                                                   " boulev$| BOULEV$", " dr$| DR$", " parkwa$| PARKWA$"],
                                       value=[" ROAD", " AVENUE", " STREET", " BOULEVARD", " DRIVE", " PARKWAY"], regex=True, inplace=True)

    red_light_frame["STREET_NO_DIR"] = red_light_frame["ADDRESS"].str.split(' ', 2)

    red_light_frame["STREET_NAME"] = red_light_frame["ADDRESS"].str.split().str[2:]
    red_light_frame["STREET_NAME"] = [' '.join(map(str, l)) for l in red_light_frame['STREET_NAME']]

    for row in range(len(red_light_frame["STREET_NO_DIR"])):
        red_light_frame["STREET_NO_DIR"][row] = red_light_frame["STREET_NO_DIR"][row][:-1]

    red_light_frame[['STREET_NO', 'STREET_DIR']] = pd.DataFrame(red_light_frame.STREET_NO_DIR.tolist(), index=red_light_frame.index)


    # For Traffic Crashes
    traffic_frame["STREET_NAME"].replace(to_replace=[" rd$| RD$", " ave$| AVE$", " st$| ST$",
                                                           " blvd$| BLVD$", " dr$| DR$", " pkwy$| PKWY$"],
                                               value=[" ROAD", " AVENUE", " STREET", " BOULEVARD", " DRIVE", " PARKWAY"],
                                               regex=True, inplace=True)

    traffic_frame['STREET_NAME'].replace(np.nan, -9999, inplace=True)

    # For Speed Camera Violations
    speed_sample["ADDRESS"].replace(to_replace=[" rd$| RD$", " av$| AV$| ave$| AVE$", " st$| stree$| STREE$| ST$",
                                                    " blvd$| BLVD$", " dr$| DR$", " parkwa$| PARKWA$", " hwy$| HWY$"],
                                        value=[" ROAD", " AVENUE", " STREET", " BOULEVARD", " DRIVE", " PARKWAY",
                                               " HIGHWAY"],
                                        regex=True, inplace=True)

    speed_sample["STREET_NO_DIR"] = speed_sample["ADDRESS"].str.split(' ', 2)

    speed_sample["STREET_NAME"] = speed_sample["ADDRESS"].str.split().str[2:]
    speed_sample["STREET_NAME"] = [' '.join(map(str, l)) for l in speed_sample['STREET_NAME']]

    for row in range(len(speed_sample["STREET_NO_DIR"])):
        speed_sample["STREET_NO_DIR"][row] = speed_sample["STREET_NO_DIR"][row][:-1]

    speed_sample[['STREET_NO', 'STREET_DIR']] = pd.DataFrame(speed_sample.STREET_NO_DIR.tolist(),
                                                                index=speed_sample.index)

    clean_red_light_frame, clean_traffic_frame, clean_speed_frame = select_attributes(red_light_frame, traffic_frame, speed_sample)

    return clean_red_light_frame, clean_traffic_frame, clean_speed_frame


def select_attributes(red_light_frame, traffic_frame, speed_frame):
    traffic_frame.drop(["CRASH_DATE", "Time", "M"], axis=1, inplace=True)
    red_light_frame.drop(["STREET_NO_DIR", "ADDRESS"], axis=1, inplace=True)
    speed_frame.drop(["STREET_NO_DIR", "ADDRESS"], axis=1, inplace=True)
    return red_light_frame, traffic_frame, speed_frame


def insert_data_to_mongo(traffic_frame, red_light_frame, speed_frame):
    client = MongoClient('localhost', 27017)
    db = client['traffic_analysis']
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


if __name__ == '__main__':
    red_light_frame, traffic_frame, speed_frame = read_data()
    red_light_frame, traffic_frame, speed_frame = process_data(traffic_frame, red_light_frame, speed_frame)
    insert_data_to_mongo(traffic_frame, red_light_frame, speed_frame)