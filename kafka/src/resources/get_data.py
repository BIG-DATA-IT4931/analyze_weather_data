import cdsapi
import os
import xarray as xr
import pandas as pd
import glob
import csv
from datetime import datetime
from datetime import timedelta
import shutil
def fetch_weather_data(datetime_obj):
    client = cdsapi.Client()
    datetime_obj = datetime_obj - timedelta(days=10)
    year = datetime_obj.year
    month = datetime_obj.month
    day = datetime_obj.day
    hour = datetime_obj.hour

    request = {
        'product_type': ['reanalysis'],
        'variable': ['10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature', 
                      '2m_temperature', 'mean_sea_level_pressure', 'sea_surface_temperature', 
                      'surface_pressure', 'total_cloud_cover', 'total_column_cloud_ice_water', 
                      'total_column_cloud_liquid_water'],
        'year': [str(year)],
        'month': [f"{month:02d}"],
        'day': [f"{day:02d}"],
        'time': [f"{hour:02d}:00"],
        'data_format': 'grib',
        'download_format': 'unarchived',
        'area': [24, 102, 8, 112]
    }
    
    folder_path = f"{year}_{month:02d}_{day:02d}_{hour:02d}"
    os.makedirs(folder_path, exist_ok=True)

    target = f"{folder_path}/other_data_{hour:02d}_{day:02d}_{month:02d}_{year}.grib"
    
    client.retrieve("reanalysis-era5-single-levels", request, target)
    
    ds = xr.open_dataset(target)
    df = ds.to_dataframe().reset_index()
    
    records = []
    for _, row in df.iterrows():
        record_id = f"{row['time']}_{row['latitude']}_{row['longitude']}"  
        record_data = ', '.join(map(str, row)) 
        records.append((record_id, record_data))
    shutil.rmtree(folder_path) 
    return records

