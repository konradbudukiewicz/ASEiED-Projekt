from rasterio.plot import show
from rasterio.io import MemoryFile
import numpy as np
from pyspark.sql import SparkSession
from pyspark import SparkContext
import geopandas as gpd
from pyspark import SparkConf, SparkContext
from shapely.geometry import Polygon
import numpy as np
import matplotlib.pyplot as plt
#import s3fs
import io


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


def plot_results(df):

    geo_df = spark_df_to_geopandas_df_for_points(df)

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
    ax1.set_xlim(-1250000, 4500000)
    ax1.set_ylim(4300000, 11000000)
    plt.savefig('foo.pdf')


"""     img_data = io.BytesIO()
    plt.savefig(img_data, format='png', bbox_inches='tight')
    img_data.seek(0)

    s3 = s3fs.S3FileSystem(anon=False)  # Uses default credentials
    with s3.open('s3://bucketpath/foo.png', 'wb') as f:
        f.write(img_data.getbuffer())
 """


def get_paths():
    paths = ""
    zoom = 8
    """
    for x in range(14, 19):
        for y in range(6, 13):
    """
    for x in range(14, 15):
        for y in range(6, 7):
            #paths = paths + f"s3://elevation-tiles-prod/geotiff/{zoom}/{x}/{y}.tif,"
            paths = paths + f"data/{x}x{y}.tif,"
    # usunięcie ostatniego przecinka
    paths = paths[:-1]
    return paths


def img_get_altitude_change(byte):
    k_indexes = 10
    with MemoryFile(byte) as memfile:
        with memfile.open() as dt:
            data_array = dt.read()
            data_array = np.array(data_array)
            data_array = data_array.flatten()
            data_array = np.sort(data_array)
            n_lowest = data_array[:k_indexes]
            n_highest = data_array[-k_indexes:]
            lowest_avg = np.average(n_lowest)
            highest_avg = np.average(n_highest)
            return float(np.abs((highest_avg - lowest_avg)) / 2)


def get_bounds(byte):
    with MemoryFile(byte) as memfile:
        with memfile.open() as dt:
            return [dt.bounds[0], dt.bounds[1], dt.bounds[2], dt.bounds[3]]


conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.1')
sc = SparkContext(conf=conf)

# zmienić klusze na swoje i aktualne
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "ASIA3WAKSRNCVG5FW6OH")
sc._jsc.hadoopConfiguration().set("fsa.s3.secret.key",
                                  "8tC5KO1iLRFTLbo5yruHJC+9frdK0lq1pMlX88sH")
spark = SparkSession.builder.appName("TerrainTiles").getOrCreate()

paths = get_paths()

rdd = sc.binaryFiles(paths)
rdd_bytes = rdd.map(lambda x: bytes(x[1]))
rdd_union = rdd_bytes.map(lambda x: get_bounds(x))
rdd_bounds = rdd_bytes.map(lambda x: get_bounds(x))
rdd_union = rdd_union.union(rdd_bounds)
rdd_altitude_change = rdd_bytes.map(lambda x: img_get_altitude_change(x))
rdd_zipped = rdd_bounds.zip(rdd_altitude_change)
data_column_names = ["bounds", "altitude change"]
df = rdd_zipped.toDF(data_column_names)
df.show()

# zapisywanie obrazka z mapą
plot_results(df)
