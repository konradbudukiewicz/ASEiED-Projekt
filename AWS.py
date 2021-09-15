import os
import boto3
import rasterio
from rasterio.plot import show
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon
import json

if __name__ == "__main__":

    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('elevation-tiles-prod')
    bounds = []
    altitude_change = []
    zoom = 10
    k_indexes = 10

    # zakresy europy
    """
    for x in range(485, 486):
        for y in range(220, 401):
    """
    #zakres dla polski
    for x in range(553, 579):
        for y in range(325, 351):
            # do pobierania danych
            # my_bucket.download_file(f"geotiff/{zoom}/{x}/{y}.tif",
            #                        f"data/{x}x{y}.tif")
            src = rasterio.open(f"data/{x}x{y}.tif")

            # print(src.bounds)
            # krawędzie obszaru (dwa punkty) zapisywane do tablicy
            bounds.append(
                [src.bounds[0], src.bounds[1], src.bounds[2], src.bounds[3]])
            data_array = src.read(1)

            # wyznaczanie wzrostu wysokości na podstawie 10 najniższych i najwyższych punktów
            data_array = np.array(data_array)
            data_array = data_array.flatten()
            data_array = np.sort(data_array)
            n_lowest = data_array[:k_indexes]
            n_highest = data_array[-k_indexes:]
            lowest_avg = np.average(n_lowest)
            highest_avg = np.average(n_highest)
            altitude_change.append(
                float(np.abs((highest_avg - lowest_avg)) / 2))

    spark = SparkSession.builder.appName("project").getOrCreate()
    sc = spark.sparkContext

    data_column_names = ["bounds", "altitude change"]
    data = list(zip(bounds, altitude_change))
    rdd = sc.parallelize(data)
    df = rdd.toDF(data_column_names)

    # zapisywanie danych do json
    df.write.json("data.json")
