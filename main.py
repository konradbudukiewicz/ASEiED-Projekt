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
import s3fs
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
    # rysowanie mapy
    geo_df = spark_df_to_geopandas_df_for_points(df)
    # granice europy
    map = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
    map = map.to_crs(epsg=3857)

    # rysowanie
    fig, ax1 = plt.subplots(1, 1)
    cmap = plt.get_cmap('RdBu', 5).reversed()
    geo_df.plot(column='group', ax=ax1, cmap=cmap, alpha=1)
    map.boundary.plot(edgecolor="white",
                      markersize=1,
                      linewidth=1,
                      alpha=0.4,
                      ax=ax1)
    ax1.set_facecolor((0.1, 0.1, 0.1))
    # ustawienie granic europy
    ax1.set_xlim(-1700000, 4400000)
    ax1.set_ylim(3800000, 11000000)
    #plt.savefig('map.pdf')

    img_data = io.BytesIO()
    plt.savefig(img_data, format='png', bbox_inches='tight')
    img_data.seek(0)

    s3 = s3fs.S3FileSystem(anon=False)  # Uses default credentials
    with s3.open('s3://bucketpath/map.png', 'wb') as f:
        f.write(img_data.getbuffer())


def get_paths():
    # generowanie ścieżek do pobrania plików
    paths = ""
    zoom = 7
    for x in range(58, 78):
        for y in range(28, 53):
            paths = paths + f"s3://elevation-tiles-prod/geotiff/{zoom}/{x}/{y}.tif,"
    # usunięcie ostatniego przecinka
    paths = paths[:-1]
    return paths


def img_get_altitude_change(byte):
    # obliczanie zmiany wysokości
    k_indexes = 10  # ilość punktów, które mają być brana pod uwagę, przy obliczaniu wyskości
    with MemoryFile(byte) as memory_file:
        with memory_file.open() as data:
            data_array = data.read()
            # zamiana rozmiaru tablicy
            data_array = np.squeeze(data_array)
            alt_data = []
            scale = 32
            # odczytywanie danych o wysokości z kolejnych części tablicy
            # zmiana wyskości obliczana jest na podstawie różnicy ze średniej wysokości k największych i k najmniejszych wartości
            for x in range(scale):
                for y in range(scale - 1, -1, -1):
                    small_data_arrray = data_array[y * 16:(y + 1) * 16,
                                                   x * 16:(x + 1) * 16]
                    small_data_arrray = small_data_arrray.flatten()
                    small_data_arrray = np.sort(small_data_arrray)
                    n_lowest = small_data_arrray[:k_indexes]
                    n_highest = small_data_arrray[-k_indexes:]
                    lowest_avg = np.average(n_lowest)
                    highest_avg = np.average(n_highest)
                    alt_data.append(
                        float(np.abs((highest_avg - lowest_avg)) / 2))
            return alt_data


def get_bounds(byte):
    # obszar z danego kwadratu, dzielony jest na mniejsze obaszary
    with MemoryFile(byte) as memory_file:
        with memory_file.open() as data:
            bounds = []
            scale = 32  # ile kortnie ma być więcej kwadratów jednej osi
            edge_length = np.abs(data.bounds[0] - data.bounds[2]) / scale
            # obliczanie granic kwadratów (2 punkty- w lewym dolnym i prawym górnym rogu)
            for x in range(scale):
                for y in range(scale):
                    bounds.append([
                        float(data.bounds[0] + x * edge_length),
                        float(data.bounds[1] + y * edge_length),
                        float(data.bounds[0] + (x + 1) * edge_length),
                        float(data.bounds[1] + (y + 1) * edge_length)
                    ])
            return bounds


def group_by_altitude(rdd_altitude_change):
    # przyporządkowuje dane do danego obszaru
    if rdd_altitude_change < 200:
        return 0
    elif rdd_altitude_change < 350:
        return 1
    elif rdd_altitude_change < 700:
        return 2
    elif rdd_altitude_change < 1000:
        return 3
    elif rdd_altitude_change >= 1000:
        return 4
    else:
        return 0


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
rdd_bin = rdd.map(lambda x: bytes(x[1]))
rdd_bounds = rdd_bin.map(lambda x: get_bounds(x))
rdd_altitude_change = rdd_bin.map(lambda x: img_get_altitude_change(x))

# zamiana rozmiaru rdd- tak, aby każdemu kwadratowi odpowiadała jedna wartość zmiany wysokości
rdd_altitude_change = rdd_altitude_change.flatMap(lambda xs: [x for x in xs])
rdd_bounds = rdd_bounds.flatMap(lambda xs: [x for x in xs])
rdd_group = rdd_altitude_change.map(group_by_altitude)
# łącznie rdd
rdd_zipped = rdd_bounds.zip(rdd_group)
# tworzenie df z rdd
data_column_names = ["bounds", "group"]
df = rdd_zipped.toDF(data_column_names)
df.show()

# zapisywanie obrazka z mapą
plot_results(df)
