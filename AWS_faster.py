from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf, SparkContext
import rasterio
import numpy as np


def get_paths():
    paths = []
    zoom = 10
    """
    for x in range(485, 486):
        for y in range(220, 401):
    """
    for x in range(553, 554):
        for y in range(325, 328):
            paths.append(
                f"s3://elevation-tiles-prod/geotiff/{zoom}/{x}/{y}.tif")
    return paths


def img_get_bounds(path):
    # krawędzie obszaru (dwa punkty) zapisywane do tablicy
    with rasterio.open(path) as img:
        return [img.bounds[0], img.bounds[1], img.bounds[2], img.bounds[3]]


def img_get_altitude_change(path):
    # wyznaczanie wzrostu wysokości na podstawie 10 najniższych i najwyższych punktów
    k_indexes = 10
    with rasterio.open(path) as img:
        data_array = img.read(1)
        data_array = np.array(data_array)
        data_array = data_array.flatten()
        data_array = np.sort(data_array)
        n_lowest = data_array[:k_indexes]
        n_highest = data_array[-k_indexes:]
        lowest_avg = np.average(n_lowest)
        highest_avg = np.average(n_highest)
        return float(np.abs((highest_avg - lowest_avg)) / 2)


if __name__ == "__main__":

    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.1')
    sc = SparkContext(conf=conf)

    # zmienić klusze na swoje i aktualnex
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key",
                                      "ASIA3WAKSRNCVG5FW6OH")
    sc._jsc.hadoopConfiguration().set(
        "fsa.s3.secret.key", "8tC5KO1iLRFTLbo5yruHJC+9frdK0lq1pMlX88sH")

    spark = SparkSession.builder.appName("project").getOrCreate()
    paths = get_paths()
    f = sc.parallelize(paths)
    rdd_bounds = f.map(img_get_bounds)
    rdd_altitude_change = f.map(img_get_altitude_change)
    rdd_zipped = rdd_bounds.zip(rdd_altitude_change)

    data_column_names = ["bounds", "altitude change"]
    df = rdd_zipped.toDF(data_column_names)
    df.show()

    df.write.mode('overwrite').json("s3a://examplexesttest/data.json")
