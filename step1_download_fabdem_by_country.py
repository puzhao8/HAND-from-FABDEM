

import os
import zipfile
import requests
from tqdm import tqdm
import geopandas as gpd
from pathlib import Path
import concurrent.futures
from prettyprinter import pprint

import ee 
ee.Initialize()

def download_file(url, local_filename):
    # Send a GET request to the URL
    with requests.get(url, stream=True) as response:
        # Raise an exception for any HTTP errors
        response.raise_for_status()
        # Open a local file with write-binary mode
        with open(local_filename, 'wb') as file:
            # Iterate over the response data and write it to the file
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    return local_filename


def download_file_with_progress(url, local_filename):
    # Send a GET request to the URL
    with requests.get(url, stream=True) as response:
        # Raise an exception for any HTTP errors
        response.raise_for_status()
        # Get the total file size from the response headers
        total_size = int(response.headers.get('content-length', 0))
        # Initialize the progress bar
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=local_filename, ascii=True) as progress_bar:
            # Open a local file with write-binary mode
            with open(local_filename, 'wb') as file:
                # Iterate over the response data and write it to the file
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
                    progress_bar.update(len(chunk))
    return local_filename


def download_files_in_parallel(urls, dst_folder):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for url in urls:
            local_filename = os.path.join(dst_folder, url.split('/')[-1])
            futures.append(executor.submit(download_file_with_progress, url, local_filename))
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error occurred: {e}")


def unzip_file(zip_filepath, extract_to):
    # Create the directory if it does not exist
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)

    # Open the ZIP file
    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        # Extract all the contents into the specified directory
        zip_ref.extractall(extract_to)
        print(f"Extracted {zip_filepath} to {extract_to}")

def unzip_all_files_in_folder(folder, extract_to):
    for filename in os.listdir(folder):
        if filename.endswith('.zip'):
            zip_filepath = os.path.join(folder, filename)
            unzip_file(zip_filepath, extract_to)


# codes in GEE: https://code.earthengine.google.com/ef186d656b039d80017cbd5ab53204cb
def query_by_country(country_name='Italy', bufferSize=-10000):
    basin5 = ee.FeatureCollection("WWF/HydroATLAS/v1/Basins/level05")
    adm_lv0 = ee.FeatureCollection("FAO/GAUL_SIMPLIFIED_500m/2015/level0")
    fabdem_tiles = ee.FeatureCollection("projects/global-wetland-watch/assets/FABDEM_v1-2_tiles")

    # filter basins at level 5 by country boundary
    country = adm_lv0.filter(ee.Filter.eq("ADM0_NAME", country_name))
    roi = country.union().geometry().buffer(bufferSize)
    basin5_filtered_by_country = basin5.filter(ee.Filter.intersects('.geo', roi))
    basin_ids = basin5_filtered_by_country.aggregate_array('HYBAS_ID').distinct().getInfo()
    # print(basin5_filtered_by_country.aggregate_array('HYBAS_ID').distinct(), 'basin5 ids')

    # filter zipfile to be downloaded for the filtered basins at level 5
    tiles_filtered = fabdem_tiles.filterBounds(basin5_filtered_by_country.geometry())
    zipFileList = tiles_filtered.aggregate_array("zipfile_na").distinct().getInfo()
    
    print("zipFiles to be downloaded.")
    pprint(zipFileList)

    return zipFileList, basin_ids


if __name__ == "__main__":

    # FABDEM: https://data.bris.ac.uk/data/dataset/s5hqmjcdj8yo2ibzi9b4ew3sn
    url_root = "https://data.bris.ac.uk/datasets/s5hqmjcdj8yo2ibzi9b4ew3sn/" 

    # region = "sa"

    # hydroBASIN = gpd.read_file(f"hydroBASIN/hybas_{region}_lev05_v1c.zip")
    # tiles = gpd.read_file("data/FABDEM_v1-2_tiles.geojson")

    # tiles_filtered = gpd.sjoin(hydroBASIN, tiles, how='inner', predicate='intersects')
    # zipFileList = tiles_filtered.zipfile_name.unique()
    # print(f"number of tiles in {region}: {len(zipFileList)}")

    region = 'Italy'
    zipFileList, basin_ids = query_by_country(country_name=region)
    

    existedList = os.listdir("data/FABDEM/zips")
    # zipFileList = [f for f in zipFileList if f not in existedList]

    print("--------------- after removing existed ones ------------")
    print(f"zipFileList len: {len(zipFileList)}")
    print("zipFileList")
    pprint(zipFileList)

    if True:
        # download zipfiles
        url_list = [url_root + zipFile for zipFile in zipFileList]
        dst_folder = Path(f"data/FABDEM/zips")
        dst_folder.mkdir(exist_ok=True, parents=True)
        download_files_in_parallel(urls=url_list, dst_folder=dst_folder)

        # extract zip files into folder 
        tile_folder = Path(f"data/FABDEM/tiles")
        
        # unzip_all_files_in_folder(dst_folder, dem_folder)

        # extract zip files one by one
        for filename in zipFileList:
            if filename.endswith('.zip'):
                zip_filepath = os.path.join("data/FABDEM/zips", filename)
                unzip_file(zip_filepath, tile_folder)
