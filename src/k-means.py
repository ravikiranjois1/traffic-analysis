from sklearn.cluster import KMeans
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler


def experiment(red_light_frame, traffic_frame, speed_frame):
    # print(red_light_frame.columns)
    # x = red_light_frame[['VIOLATION DATE', 'VIOLATIONS']].reset_index(drop=True, inplace=True)
    # print(x)

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



    speed_frame_sample = speed_frame[['STREET_NAME', 'VIOLATIONS']].groupby('STREET_NAME', as_index=False).sum()
    red_light_frame_sample = red_light_frame[['STREET_NAME', 'VIOLATIONS']].groupby('STREET_NAME', as_index=False).sum()
    traffic_frame_sample = traffic_frame[['STREET_NAME', 'Date']].groupby('STREET_NAME', as_index=False).count()

    red_light_frame_sample.columns = ["STREET_NAME", "REDLIGHT_VIOLATIONS"]
    speed_frame_sample.columns = ["STREET_NAME", "SPEED_VIOLATIONS"]
    res = pd.merge(traffic_frame_sample, speed_frame_sample, how='left', on='STREET_NAME')
    res = pd.merge(res, red_light_frame_sample, how='left', on='STREET_NAME')


    res["Total_violation"] = res["REDLIGHT_VIOLATIONS"] + res["SPEED_VIOLATIONS"]
    res['REDLIGHT_VIOLATIONS'].fillna(0, inplace=True)
    res['SPEED_VIOLATIONS'].fillna(0, inplace=True)
    res['Total_violation'].fillna(0, inplace=True)
    res.columns = ["STREET_NAME", "Crashes", "SPEED_VIOLATIONS", "REDLIGHT_VIOLATIONS", "Total_violation"]
    print(res)
    # res = res[["Crashes", "SPEED_VIOLATIONS", "REDLIGHT_VIOLATIONS", "Total_violation"]]
    res = res[["Crashes", "REDLIGHT_VIOLATIONS"]]

    # scaler = StandardScaler()
    # scaler.fit(res)
    # norm = scaler.transform(res)
    norm = res.to_numpy()
    # norm = res.iloc[:, [2, 4]]
    print(norm)

    kmeans = KMeans(n_clusters=3)
    y = kmeans.fit(norm)
    y_kmeans = kmeans.predict(norm)

    plt.scatter(norm[:, 0], norm[:, 1], c=y_kmeans, s=10, cmap='viridis')
    centers = kmeans.cluster_centers_
    plt.scatter(centers[:, 0], centers[:, 1], c='black', s=200, alpha=0.5)
    plt.show()

    # print(len(y))


if __name__ == "__main__":
    red_light_frame = pd.read_csv(
        "/Users/abhayrajendradixit/Documents/Assignments and Projects/Projects/Data Cleaning Project/refined_redlight.csv")
    traffic_frame = pd.read_csv(
        "/Users/abhayrajendradixit/Documents/Assignments and Projects/Projects/Data Cleaning Project/refined_traffic.csv")
    speed_frame = pd.read_csv(
        "/Users/abhayrajendradixit/Documents/Assignments and Projects/Projects/Data Cleaning Project/refined_speed")

    experiment(red_light_frame, traffic_frame, speed_frame)