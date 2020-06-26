import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'


def read_data():
    traffic_crashes = "/Users/ravikiranjois/Documents/RIT/Semester 3 - Summer '20/Data Cleaning and Preparation/Project/Datasets/Traffic_Crashes.csv"
    red_light_violations = "/Users/ravikiranjois/Documents/RIT/Semester 3 - Summer '20/Data Cleaning and Preparation/Project/Datasets/Red_Light_Camera_Violations.csv"

    red_light_df = pd.read_csv(red_light_violations, usecols=["ADDRESS", "VIOLATION DATE", "VIOLATIONS"])

    crash_df = pd.read_csv(traffic_crashes,
                           usecols=["CRASH_DATE", "STREET_NO", "STREET_NAME", "STREET_DIRECTION", "POSTED_SPEED_LIMIT",
                                    "FIRST_CRASH_TYPE", "TRAFFICWAY_TYPE", "PRIM_CONTRIBUTORY_CAUSE"])

    red_light_sample = pd.DataFrame(red_light_df.ADDRESS.str.split(' ', 2).tolist(),
                                    columns=['STREET_NO', 'STREET_DIRECTION', 'STREET_NAME'])

    crash_df[['Date', 'Time', 'M']] = crash_df.CRASH_DATE.str.split(" ", expand=True, )

    red_light_df_sample = red_light_df.head(10000)
    crash_df_sample = crash_df.head(10000)

    red_light_df_sample.to_csv("/Users/ravikiranjois/Documents/RIT/Semester 3 - Summer '20/Data Cleaning and Preparation/Project/Datasets/Red_Light_Camera_Violations_sample.csv",
        sep='\t', encoding='utf-8')
    crash_df_sample.to_csv("/Users/ravikiranjois/Documents/RIT/Semester 3 - Summer '20/Data Cleaning and Preparation/Project/Datasets/Traffic_Crashes_sample.csv",
        sep='\t', encoding='utf-8')

    # print(red_light_df_sample)
    # print(crash_df_sample)
    return red_light_df_sample, crash_df_sample


def process_data(traffic_frame, red_light_frame):
    red_light_frame["TYPE"] = red_light_frame["ADDRESS"].str.split().str[-1]
    red_light_address_types = set()
    for item in red_light_frame.TYPE:
        red_light_address_types.add(item)
    print(red_light_address_types)

    red_light_frame["STREET_NO_DIR"] = red_light_frame["ADDRESS"].str.split(' ', 2)
    for row in range(len(red_light_frame["STREET_NO_DIR"])):
        red_light_frame["STREET_NO_DIR"][row] = red_light_frame["STREET_NO_DIR"][row][:-1]

    red_light_frame["STREET_NAME"] = red_light_frame["ADDRESS"].str.split().str[2:]
    for row in range(len(red_light_frame["STREET_NAME"])):
        if red_light_frame["STREET_NAME"][row][-1] == "ROA":
            red_light_frame["STREET_NAME"][row][-1] = "ROAD"
            red_light_frame["STREET_NAME"][row] = " ".join(red_light_frame["STREET_NAME"][row])
        elif red_light_frame["STREET_NAME"][row][-1] == "AVEN":
            red_light_frame["STREET_NAME"][row][-1] = "AVENUE"
            red_light_frame["STREET_NAME"][row] = " ".join(red_light_frame["STREET_NAME"][row])
        elif red_light_frame["STREET_NAME"][row][-1] == "STREE":
            red_light_frame["STREET_NAME"][row][-1] = "STREET"
            red_light_frame["STREET_NAME"][row] = " ".join(red_light_frame["STREET_NAME"][row])
        elif red_light_frame["STREET_NAME"][row][-1] == "BOULEV":
            red_light_frame["STREET_NAME"][row][-1] = "BOULEVARD"
            red_light_frame["STREET_NAME"][row] = " ".join(red_light_frame["STREET_NAME"][row])
        elif red_light_frame["STREET_NAME"][row][-1] == "DR":
            red_light_frame["STREET_NAME"][row][-1] = "DRIVE"
            red_light_frame["STREET_NAME"][row] = " ".join(red_light_frame["STREET_NAME"][row])
        elif red_light_frame["STREET_NAME"][row][-1] == "PARKWA":
            red_light_frame["STREET_NAME"][row][-1] = "PARKWAY"
            red_light_frame["STREET_NAME"][row] = " ".join(red_light_frame["STREET_NAME"][row])
        else:
            red_light_frame["STREET_NAME"][row] = " ".join(red_light_frame["STREET_NAME"][row])
    # print(red_light_frame)

    traffic_frame["STREET_FIRST_NAME"] = traffic_frame["STREET_NAME"].str.split(' ', -1)

    traffic_crash_types = set()
    # i=0
    traffic_name_df = traffic_frame[pd.notnull(traffic_frame["STREET_FIRST_NAME"])]
    for item in traffic_name_df.STREET_FIRST_NAME:
        # i+=1
        # print(i, item, item[-1])
        traffic_crash_types.add(item[-1])
    print(traffic_crash_types)

    for row in range(len(traffic_frame["STREET_FIRST_NAME"])):
        if traffic_frame["STREET_FIRST_NAME"][row].isnull():
            continue
        if traffic_frame["STREET_FIRST_NAME"][row][-1] == "RD":
            traffic_frame["STREET_FIRST_NAME"][row][-1] = "ROAD"
            traffic_frame["STREET_FIRST_NAME"][row] = " ".join(traffic_frame["STREET_FIRST_NAME"][row])
        elif traffic_frame["STREET_FIRST_NAME"][row][-1] == "AVE":
            traffic_frame["STREET_FIRST_NAME"][row][-1] = "AVENUE"
            traffic_frame["STREET_FIRST_NAME"][row] = " ".join(traffic_frame["STREET_FIRST_NAME"][row])
        elif traffic_frame["STREET_FIRST_NAME"][row][-1] == "ST":
            traffic_frame["STREET_FIRST_NAME"][row][-1] = "STREET"
            traffic_frame["STREET_FIRST_NAME"][row] = " ".join(traffic_frame["STREET_FIRST_NAME"][row])
        elif traffic_frame["STREET_FIRST_NAME"][row][-1] == "BLVD":
            traffic_frame["STREET_FIRST_NAME"][row][-1] = "BOULEVARD"
            traffic_frame["STREET_FIRST_NAME"][row] = " ".join(traffic_frame["STREET_FIRST_NAME"][row])
        elif traffic_frame["STREET_FIRST_NAME"][row][-1] == "DR":
            traffic_frame["STREET_FIRST_NAME"][row][-1] = "DRIVE"
            traffic_frame["STREET_FIRST_NAME"][row] = " ".join(traffic_frame["STREET_FIRST_NAME"][row])
        elif traffic_frame["STREET_FIRST_NAME"][row][-1] == "PKWY":
            traffic_frame["STREET_FIRST_NAME"][row][-1] = "PARKWAY"
            traffic_frame["STREET_FIRST_NAME"][row] = " ".join(traffic_frame["STREET_FIRST_NAME"][row])
        else:
            traffic_frame["STREET_FIRST_NAME"][row] = " ".join(traffic_frame["STREET_FIRST_NAME"][row])
    print(traffic_frame)
        # if name[-1] == "ROA":
        #     name[-1] = "ROAD"
        # name = [" ".join(name)]
        # print("x")
    # for name in red_light_frame["STREET_NAME"]:
    #     x = 1
    #     if name[-1] == "ROA":
    #         name[-1] = "ROAD"
    #     elif name[-1] == "AVEN":
    #         name[-1] = "AVENUE"
    #     elif name[-1] == "STREE":
    #         name[-1] = "STREET"
    #     elif name[-1] == "BOULEV":
    #         name[-1] = "BOULEVARD"
    #     elif name[-1] == "DR":
    #         name[-1] = "DRIVE"
    #     # print("x")
    # for name in red_light_frame["STREET_NAME"]:
    #     name = [" ".join(name)]



    # for item in red_light_frame
    #     if(red_light_address_types)



if __name__ == '__main__':
    red_light_frame, traffic_frame = read_data()
    process_data(traffic_frame, red_light_frame)
