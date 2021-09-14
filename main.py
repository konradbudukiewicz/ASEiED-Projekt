import os
import boto3
import rasterio
from rasterio.plot import show
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon


def spark_df_to_geopandas_df_for_points(sdf):
    df = sdf.toPandas()
    print(df)
    print(df.Bounds)

    gdf = gpd.GeoDataFrame(df.drop(["Bounds"], axis=1),
                           crs={'init': 'epsg:3857'},
                           geometry=[
                               Polygon([(bound[0], bound[1]),
                                        (bound[0], bound[3]),
                                        (bound[2], bound[3]),
                                        (bound[2], bound[1])])
                               for bound in df.Bounds
                           ])
    return gdf


if __name__ == "__main__":
    # zakresy europy
    """
    for x in range(485, 486):
        for y in range(220, 401):
    """
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('elevation-tiles-prod')
    bounds = []
    mean = []
    zoom = 10

    #zakres dla polski
    for x in range(553, 579):
        for y in range(325, 351):
            # do pobierania danych
            #my_bucket.download_file(f"geotiff/{zoom}/{x}/{y}.tif",
            #                        f"data/{x}x{y}.tif")
            src = rasterio.open(f"data/{x}x{y}.tif")

            #print(src.bounds)
            bounds.append(
                [src.bounds[0], src.bounds[1], src.bounds[2], src.bounds[3]])
            array = src.read(1)
            mean.append(float(np.mean(array)))
            #print(np.mean(array))
            #print(mean)
            #print(array)
            #print(mean)
            #print(bounds)
            #plt.imshow(array, cmap='Greens')
            #plt.show()
    spark = SparkSession.builder.appName("project").getOrCreate()
    sc = spark.sparkContext
    data_column_names = ["Bounds", "Mean"]
    data = list(zip(bounds, mean))
    rdd = sc.parallelize(data)
    #print(rdd)
    df = rdd.toDF(data_column_names)
    #df.show(df.count(), False)
    #df.select("Mean").show()

    geo_df = spark_df_to_geopandas_df_for_points(df)
    print(geo_df)

    #granice europy
    map = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
    map = map.to_crs(epsg=3857)

    #rysowanie
    fig, ax1 = plt.subplots(1, 1)
    geo_df.plot(column='Mean', ax=ax1, cmap="hot_r", alpha=1, legend=True)
    map.boundary.plot(edgecolor="gray",
                      markersize=1,
                      linewidth=1,
                      alpha=1,
                      ax=ax1)
    ax1.set_facecolor((0.1, 0.1, 0.1))
    ax1.set_xlim(-1500000, 5500000)
    ax1.set_ylim(4300000, 11500000)
    plt.show()
