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


def spark_df_to_geopandas_df_for_points(sdf):
    # zamiana df ze spark na geopandas
    df = sdf.toPandas()
    gdf = gpd.GeoDataFrame(df.drop(["bounds"], axis=1),
                           crs={'init': 'epsg:3857'},
                           geometry=[
                               Polygon([(bound[0], bound[1]),
                                        (bound[0], bound[3]),
                                        (bound[2], bound[3]),
                                        (bound[2], bound[1])])
                               for bound in df.bounds
                           ])
    return gdf


if __name__ == "__main__":

    spark = SparkSession.builder.appName("project").getOrCreate()
    sc = spark.sparkContext
    # czytanie danych z json
    df = spark.read.json("data.json")

    geo_df = spark_df_to_geopandas_df_for_points(df)

    print(geo_df)

    # granice europy
    map = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
    map = map.to_crs(epsg=3857)

    # rysowanie
    fig, ax1 = plt.subplots(1, 1)
    geo_df.plot(column='altitude change',
                ax=ax1,
                cmap="hot_r",
                alpha=1,
                legend=True)
    map.boundary.plot(edgecolor="gray",
                      markersize=1,
                      linewidth=1,
                      alpha=1,
                      ax=ax1)
    ax1.set_facecolor((0.1, 0.1, 0.1))
    # ustawienie granic europy
    ax1.set_xlim(-1500000, 5500000)
    ax1.set_ylim(4300000, 11500000)
    plt.show()
