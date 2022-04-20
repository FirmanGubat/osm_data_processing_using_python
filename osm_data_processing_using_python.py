from sqlalchemy import create_engine
import geopandas as gpd
import geoalchemy2
import psycopg2
import os, sys, subprocess
import requests
import zipfile
from io import BytesIO
import wget

def download():
    try:
        #Defining the zip file URL
        url = 'http://download.geofabrik.de/asia/indonesia/java-latest-free.shp.zip'
        wget.download(url)
        with zipfile.ZipFile('path/java-latest-free.shp.zip', 'r') as zip_ref:
            zip_ref.extractall('directory folder for unzip file')
    except Exception as e:
        print("Download or Extract Failed: " + str(e))

def clip():
    try:
        batas_adm_jabar = gpd.read_file('path polygon file/name.shp')

        base_dir = r"directory folder shapefile"
        full_dir = os.walk(base_dir)
        shapefile_list = []
        for source, dirs, files in full_dir:
            for file in files:
                if file[-3:] == 'shp':
                    shapefile_path = os.path.join(base_dir, file)
                    shapefile_list.append(shapefile_path)

        for shape_path in shapefile_list:
            gdf = gpd.read_file(shape_path)
            clip = gpd.clip(gdf, batas_adm_jabar)
            clip.to_file('directory for clip data' + os.path.basename(shape_path))
    except Exception as e:
        print("Clip Failed: " + str(e))

def load():
    try:
        os.environ['PATH'] += r'/lib/postgresql/12/bin/'
        os.environ['PGHOST'] = 'host'
        os.environ['PGPORT'] = 'port'
        os.environ['PGUSER'] = 'username'
        os.environ['PGPASSWORD'] = 'pass'
        os.environ['PGDATABASE'] = 'database_name'

        base_dir = r"directory clip data"
        full_dir = os.walk(base_dir)
        shapefile_list = []
        for source, dirs, files in full_dir:
            for file in files:
                if file[-3:] == 'shp':
                    shapefile_path = os.path.join(base_dir, file)
                    shapefile_list.append(shapefile_path)

        for shape_path in shapefile_list:
            cmds = 'shp2pgsql "' + shape_path + '" schema."' + os.path.basename(shape_path[:-4]) + '" | psql '
            subprocess.call(cmds, shell=True)
    except Exception as e:
        print("Load Failed: " + str(e))

try:
    #call function
    download()
    clip()
    load()
except Exception as e:
    print("Error: " + str(e))