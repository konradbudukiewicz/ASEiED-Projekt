import boto3
import rasterio
from rasterio.plot import show
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import numpy as np
import os

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
    for x in range(553, 554):
        for y in range(325, 328):
            print("File %s x %s" % (x, y))
            # do pobierania danych
            # my_bucket.download_file(f"geotiff/{zoom}/{x}/{y}.tif",
            #                        f"data/{x}x{y}.tif")
            src = rasterio.open(
                f"s3://elevation-tiles-prod/geotiff/{zoom}/{x}/{y}.tif")
            #src = rasterio.open(f"data/{x}x{y}.tif")

            #print(src.bounds)
            # krawędzie obszaru (dwa punkty) zapisywane do tablicy
            bounds.append(
                [src.bounds[0], src.bounds[1], src.bounds[2], src.bounds[3]])
            data_array = src.read(1)
            src.close()

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

    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.1')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName("project").getOrCreate()

    # zmienić klusze na swoje i aktualnex
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key",
                                      "ASIA3WAKSRNC5LEAAQER")
    sc._jsc.hadoopConfiguration().set(
        "fsa.s3.secret.key", "ede3cWJJBAXrl9DOB4YajmJBIgVBBESg6ITgAwTT")

    data_column_names = ["bounds", "altitude change"]
    data = list(zip(bounds, altitude_change))
    rdd = sc.parallelize(data)
    df = rdd.toDF(data_column_names)

    # zapisywanie danych do json
    df.write.mode('overwrite').json("s3a://examplexesttest/data.json")
