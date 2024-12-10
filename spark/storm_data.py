import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import time
import pandas as pd
import numpy as np
import numpy as np
import scipy as sp
import scipy.ndimage as ndimage
from pyspark.sql import functions as F
from pyspark.sql.functions import col, asc, desc
import numpy as np
from pyspark import SparkConf
from pyspark.sql import Row
from pyspark.sql.functions import lit
from datetime import datetime, timedelta
import numpy as np
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
credentials_location = '/home/phuc_0703/learnDE/data-engineering-zoomcamp/strong-ward-437213-j6-c3ae16d10e5f.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "/home/phuc_0703/learnDE/data-engineering-zoomcamp/05-batch/libs/gcs-connector.jar,/home/phuc_0703/learnDE/data-engineering-zoomcamp/05-batch/libs/bigquery-connector.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)
sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()


df_weather_main = (
    spark.read.option("timestampAsString", "true")
    .parquet("gs://weather_bigdata_20241/weather_all_main/*")
)
storm_count = 0
def distance_matrix(lons,lats):
    '''Calculates the distances (in km) between any two cities based on the formulas
    c = sin(lati1)*sin(lati2)+cos(longi1-longi2)*cos(lati1)*cos(lati2)
    d = EARTH_RADIUS*Arccos(c)
    where EARTH_RADIUS is in km and the angles are in radians.
    Source: http://mathforum.org/library/drmath/view/54680.html
    This function returns the matrix.'''

    EARTH_RADIUS = 6378.1
    X = len(lons)
    Y = len(lats)
    assert X == Y, 'lons and lats must have same number of elements'

    d = np.zeros((X,X))

    for i2 in range(len(lons)):
        lati2 = lats[i2]
        loni2 = lons[i2]
        c = np.sin(np.radians(lats)) * np.sin(np.radians(lati2)) + \
            np.cos(np.radians(lons-loni2)) * \
            np.cos(np.radians(lats)) * np.cos(np.radians(lati2))
        d[c<1,i2] = EARTH_RADIUS * np.arccos(c[c<1])

    return d

def diameter(area):
    r = np.sqrt((area/np.pi))
    return (r*2)

def major_ratio(area, ratio):
    maj = np.sqrt(area/(np.pi*ratio))
    return maj 

def major_ecc(area, ecc):
    denom = np.pi*np.sqrt(1-(ecc**2))
    maj = np.sqrt(area/denom)
    return maj

def ecc(ratio):
    e = (ratio)*(np.sqrt((ratio**-2)-1))
    return e

def nanmean(array, axis=None):
    return np.mean(np.ma.masked_array(array, np.isnan(array)), axis)

def len_deg_lon(lat):
    '''
    Returns the length of one degree of longitude (at latitude
    specified) in km.
    '''

    R = 6371. # Radius of Earth [km]

    return (np.pi/180.) * R * np.cos( lat * np.pi/180. )

def spatial_filter(msl, lon, lat, res, cut_lon, cut_lat):
    '''
    Performs a spatial filter, removing all features with
    wavelenth scales larger than cut_lon in longitude and
    cut_lat in latitude from msl (defined in grid given
    by lon and lat).  msl has spatial resolution of res
    and land identified by np.nan's
    '''

    msl_filt = np.zeros(msl.shape)

    sig_lon = (cut_lon/5.) / res
    sig_lat = (cut_lat/5.) / res

    land = np.isnan(msl)
    msl[land] = nanmean(msl)

    msl_filt =  ndimage.gaussian_filter(msl, [sig_lat, sig_lon])

    msl_filt[land] = np.nan

    return msl_filt


def detect_storms(msl, wind_speed, lon, lat, res, order, Npix_min, Npix_max, rel_amp_thresh, d_thresh, cyc, cut_lon, cut_lat, globe=False):
    '''
    Detect storms present in msl which satisfy the criteria.
    Algorithm is an adaptation of an eddy detection algorithm,
    outlined in Chelton et al., Prog. ocean., 2011, App. B.2,
    with modifications needed for storm detection.

    msl is a 2D array specified on grid defined by lat and lon.

    res is the horizontal grid resolution in degrees of msl

    Npix_min is the minimum number of pixels within which an
    extremum of msl must lie (recommended: 9).

    cyc = 'cyclonic' or 'anticyclonic' specifies type of system
    to be detected (cyclonic storm or high-pressure systems)

    globe is an option to detect storms on a globe, i.e. with periodic
    boundaries in the West/East. Note that if using this option the 
    supplied longitudes must be positive only (i.e. 0..360 not -180..+180).

    Function outputs lon, lat coordinates of detected storms
    '''

    len_deg_lat = 111.325

    msl = spatial_filter(msl, lon, lat, res, cut_lon, cut_lat)
    if globe:
        dl = 20. 
        iEast = np.where(lon >= 360. - dl)[0][0]
        iWest = np.where(lon <= dl)[0][-1]
        lon = np.append(lon[iEast:]-360, np.append(lon, lon[:iWest]+360))
        msl = np.append(msl[:,iEast:], np.append(msl, msl[:,:iWest], axis=1), axis=1)

    llon, llat = np.meshgrid(lon, lat)

    lon_storms = np.array([])
    lat_storms = np.array([])
    amp_storms = np.array([])
    area_storms = np.array([])
    max_wind_speeds = np.array([])
    regions_storm = []
    ssh_crits = np.linspace(np.nanmin(msl), np.nanmax(msl), 200)
    ssh_crits = np.array([100000])
    if order == 'topdown':
        ssh_crits = np.flipud(ssh_crits)

    for ssh_crit in ssh_crits:
 
        if cyc == 'anticyclonic':
            regions, nregions = ndimage.label( (msl>ssh_crit).astype(int) )
        elif cyc == 'cyclonic':
            regions, nregions = ndimage.label( (msl<ssh_crit).astype(int) )

        for iregion in range(nregions):
 
            region = (regions==iregion+1).astype(int)
            region_Npix = region.sum()
            storm_area_within_limits = ((region_Npix >= Npix_min) * (region_Npix <= Npix_max))
            interior = ndimage.binary_erosion(region)
            exterior = np.logical_xor(region.astype(bool), interior)
            if interior.sum() == 0:
                continue
            if cyc == 'anticyclonic':
                has_internal_ext = msl[interior].max() > msl[exterior].max()
            elif cyc == 'cyclonic':
                has_internal_ext = msl[interior].min() < msl[exterior].min()
 
            if cyc == 'anticyclonic':
                amp_abs = msl[interior].max()
                amp = amp_abs - msl[exterior].mean()
            elif cyc == 'cyclonic':
                amp_abs = msl[interior].min()
                amp = msl[exterior].mean() - amp_abs
            is_tall_storm = amp >= rel_amp_thresh
            lon_ext = llon[exterior]
            lat_ext = llat[exterior]
            d = distance_matrix(lon_ext, lat_ext)
            is_small_storm = d.max() < d_thresh
    

            if np.logical_not(storm_area_within_limits * is_tall_storm * is_small_storm):
                # print("storm_area_within_limits: " + str(storm_area_within_limits))
                # print("has_internal_ext: " + str(has_internal_ext))
                # print("is_tall_storm: " + str(is_tall_storm))
                # print("is_small_storm: " + str(is_small_storm))
                # print(str(storm_area_within_limits * has_internal_ext * is_tall_storm * is_small_storm))
                continue
 

            if (storm_area_within_limits * is_tall_storm * is_small_storm):

                storm_object_with_mass = msl * region
                storm_object_with_mass[np.isnan(storm_object_with_mass)] = 0
                min_pressure_pos = np.unravel_index(np.argmin(msl), msl.shape)
    

                j_min, i_min = min_pressure_pos
                lon_cen = np.interp(i_min, range(0,len(lon)), lon)
                lat_cen = np.interp(j_min, range(0,len(lat)), lat)

                

                lon_storms = np.append(lon_storms, lon_cen)
                lat_storms = np.append(lat_storms, lat_cen)
                
                amp_storms = np.append(amp_storms, amp_abs)
                area = region_Npix * res**2 * len_deg_lat * len_deg_lon(lat_cen) # [km**2]
                area_storms = np.append(area_storms, area)                
                storm_mask = np.ones(msl.shape)
                storm_mask[interior.astype(int)==1] = np.nan
                storm_mask = msl * storm_mask
                region_storm = np.isnan(storm_mask).astype(int)
                regions_storm.append(region_storm)
                max_wind_speed = (wind_speed*region_storm).max()
                max_wind_speeds = np.append(max_wind_speeds, max_wind_speed)

    return lon_storms, lat_storms, amp_storms, max_wind_speeds, area_storms, regions_storm, 

attributes = {
    "U10": "u10",
    "V10": "v10",
    "Mean Sea Level Pressure": "msl",
}
startTime = "2022-01-01 00:00:00"
endTime = "2024-10-31 23:00:00"

def process_partition(partition_data):
    reshaped_batch = {}
    for attr_name, attr_col in attributes.items():
        col_data = [row[attr_col] for row in partition_data]
        dim1 = len(col_data) // (65 * 41)
        reshaped_batch[attr_col] = np.reshape(col_data, (dim1, 65, 41))
        
    msl = reshaped_batch["msl"]
    lon = np.arange(102, 112.25, 0.25)
    lat = np.arange(24, 7.75,-0.25)
    u10 = reshaped_batch["u10"]
    v10 = reshaped_batch["v10"]
    wind_speed = np.sqrt(u10*u10+v10*v10)
    res = 0.25 
    order = 'topdown' 
    Npix_min = 9
    Npix_max = 6000  
    rel_amp_thresh = 100  
    d_thresh = 2500  
    cyc = 'cyclonic'  
    cut_lon = 1  #
    cut_lat = 1  
    globe = False  
    def haversine_distance(lon1, lat1, lon2, lat2):
        return np.sqrt((lon1 - lon2) ** 2 + (lat1 - lat2) ** 2)
    storm_id = None
    prev_lon, prev_lat = None, None
    rows = []
    for i in range(msl.shape[0]):
        time = datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S") + timedelta(hours=i)

        lon_storms, lat_storms, amp_storms, max_wind_speeds, area_storms, regions_storm = detect_storms(
            msl[i, :, :], wind_speed[i, :, :], lon, lat, res, order, Npix_min, Npix_max, rel_amp_thresh, 
            d_thresh, cyc, cut_lon, cut_lat, globe
        )

        if lon_storms.size == 0:
            rows.append(Row(
                time=time.strftime("%Y-%m-%d %H:%M:%S"),
                id="2024-00",
                lon_storm=None,
                lat_storm=None,
                amp_storm=None,
                max_wind_speed=None,
                area_storm=None,
                regions_storm=None
            ))
        else:
            lon_storm = lon_storms[0]
            lat_storm = lat_storms[0]
            amp_storm = amp_storms[0]
            max_wind_speed = max_wind_speeds[0]
            area_storm = area_storms[0]
            regions_storm = np.array(regions_storm)
            regions_shape = regions_storm.shape
            regions_storm_str = f"{regions_shape}_{regions_storm.flatten().tolist()}"

            if prev_lon is not None and haversine_distance(lon_storm, lat_storm, prev_lon, prev_lat) < 1:
                storm_id = f"2024-{storm_count:02d}"
            else:
                storm_count += 1
                storm_id = f"2024-{storm_count:02d}"

            prev_lon, prev_lat = lon_storm, lat_storm

            rows.append(Row(
                time=time.strftime("%Y-%m-%d %H:%M:%S"),
                id=storm_id,
                lon_storm=lon_storm,
                lat_storm=lat_storm,
                amp_storm=amp_storm,
                max_wind_speed=max_wind_speed,
                area_storm=area_storm,
                regions_storm=regions_storm_str
            ))

    schema = StructType([
        StructField("time", TimestampType(), True),
        StructField("id", StringType(), True),
        StructField("lon_storm", DoubleType(), True),
        StructField("lat_storm", DoubleType(), True),
        StructField("amp_storm", DoubleType(), True),
        StructField("max_wind_speed", DoubleType(), True),
        StructField("area_storm", DoubleType(), True),
        StructField("regions_storm", StringType(), True)
    ])

    rows_with_converted_time = [
        Row(
            time=datetime.strptime(row.time, "%Y-%m-%d %H:%M:%S"),
            id=row.id,
            lon_storm=float(row.lon_storm) if row.lon_storm is not None else None,
            lat_storm=float(row.lat_storm) if row.lat_storm is not None else None,
            amp_storm=float(row.amp_storm) if row.amp_storm is not None else None,
            max_wind_speed=float(row.max_wind_speed) if row.max_wind_speed is not None else None,
            area_storm=float(row.area_storm) if row.area_storm is not None else None,
            regions_storm=row.regions_storm
        )
        for row in rows
    ]

    schema = StructType([
        StructField("time", TimestampType(), True),
        StructField("id", StringType(), True),
        StructField("lon_storm", DoubleType(), True),
        StructField("lat_storm", DoubleType(), True),
        StructField("amp_storm", DoubleType(), True),
        StructField("max_wind_speed", DoubleType(), True),
        StructField("area_storm", DoubleType(), True),
        StructField("regions_storm", StringType(), True)
    ])

    storms_df = spark.createDataFrame(rows_with_converted_time, schema=schema)

    storms_df.show(truncate=False)
    table_name = "strong-ward-437213-j6.bigdata_20241.storms"
    storms_df.write.format("bigquery") \
        .option("table", table_name) \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()

df_weather_main = (
    spark.read.option("timestampAsString", "true")
    .parquet("gs://weather_bigdata_20241/weather_all_main/*")
)

columns = ["time", "latitude", "longitude"] + list(attributes.values())
filtered_df = df_weather_main.filter(
    (col("time") >= startTime) & (col("time") <= endTime)
).select(*columns).orderBy(asc("time"), desc("latitude"), asc("longitude"))

filtered_df.rdd.foreachPartition(lambda partition: process_partition(partition))

