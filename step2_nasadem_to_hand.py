# FABDEM: https://data.bris.ac.uk/data/dataset/s5hqmjcdj8yo2ibzi9b4ew3sn
# FABDEM in GEE: https://gee-community-catalog.org/projects/fabdem/

# Flow Accumulation Visualization: https://code.earthengine.google.com/eae949c6188239ea0108b9d61cddb9e3
# check failed hybas_id (from level-5 to level-6): https://code.earthengine.google.com/1a094d97538255a5039a6d36db002a07
# compare hand: https://code.earthengine.google.com/760177edebe0ba65bf6feb9220a886cb

# missing flow_acc: https://code.earthengine.google.com/4b60ce81f4374fc72c111c3f09748d76

"""Prepare a Copernicus GLO-30 DEM virtual raster (VRT) covering a given geometry"""
import subprocess
from pathlib import Path
from typing import Union

from osgeo import gdal, ogr
from shapely.geometry.base import BaseGeometry
from prettyprinter import pprint

from asf_tools import vector
from asf_tools.util import GDALConfigManager

import signal
import threading

DEM_GEOJSON = '/vsicurl/https://asf-dem-west.s3.amazonaws.com/v2/cop30-2021.geojson'

gdal.UseExceptions()
ogr.UseExceptions()


from shapely import wkt
from shapely.geometry import box
import pystac_client
from shapely.geometry import Polygon, shape
from pystac import ItemCollection
import planetary_computer


def query_nasadem_by_geometry(geometry):
    """Query NASADEM items using STAC API spatial search"""
    
    # Create STAC client
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1"
    )
    
    # Search for NASADEM items intersecting the geometry
    search = catalog.search(
        collections=["nasadem"],
        intersects=geometry.__geo_interface__,
        max_items=50  # Adjust based on your needs
    )
    
    items = search.item_collection()
    print(f"Found {len(items)} NASADEM items")
    
    return items

def get_dem_tiles(basin):

    if hasattr(basin, "total_bounds"):                 # GeoDataFrame / GeoSeries
        min_lon, min_lat, max_lon, max_lat = basin.total_bounds
    else:
        min_lon, min_lat, max_lon, max_lat = basin.bounds  # Shapely geometry
    
    bbox_geometry = box(min_lon, min_lat, max_lon, max_lat)

    # Query NASADEM for Armenia
    items = query_nasadem_by_geometry(bbox_geometry)

    # Sign and download items
    signed_items = [planetary_computer.sign(item) for item in items]
    tile_names = [f"{item.id}.tif" for item in signed_items]
    return tile_names


def prepare_fabdem_vrt(vrt: Union[str, Path], geometry: Union[ogr.Geometry, BaseGeometry], dem='fabdem', fabdem_path='DEM/FABDEM'):
    """Create a DEM mosaic VRT covering a given geometry

    The DEM mosaic is assembled from the Copernicus GLO-30 DEM tiles that intersect the geometry.

    Note: `asf_tools` does not currently support geometries that cross the antimeridian.

    Args:
        vrt: Path for the output VRT file
        geometry: Geometry in EPSG:4326 (lon/lat) projection for which to prepare a DEM mosaic

    """

    dem_file_names = get_dem_tiles(geometry)
    dem_file_paths = [str(fabdem_path / filename) for filename in dem_file_names if (fabdem_path / filename).exists()]

    # if 'fabdem' == dem:
    #     DEM_GEOJSON = 'data/FABDEM_v1-2_tiles.geojson'

    # with GDALConfigManager(GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR'):
    #     if isinstance(geometry, BaseGeometry):
    #         geometry = ogr.CreateGeometryFromWkb(geometry.wkb)

    #     min_lon, max_lon, _, _ = geometry.GetEnvelope()
    #     if min_lon < -160. and max_lon > 160.:
    #         raise ValueError(f'asf_tools does not currently support geometries that cross the antimeridian: {geometry}')

    #     tile_features = vector.get_features(DEM_GEOJSON)
    #     if not vector.get_property_values_for_intersecting_features(geometry, tile_features):
    #         # return 
    #         raise ValueError(f'Copernicus GLO-30 DEM does not intersect this geometry: {geometry}')


    #     if 'fabdem' == dem:
    #       dem_file_names = vector.intersecting_feature_properties(geometry, tile_features, 'file_name')

    #       # fabdem_path = Path("C:/DHI/HAND/DEM/N00W080-N10W070_FABDEM_V1-2")
    #       fabdem_path = Path(fabdem_path)
    #       dem_file_paths = [str(fabdem_path / filename) for filename in dem_file_names]

    #     else:
    #         dem_file_paths = vector.intersecting_feature_properties(geometry, tile_features, 'file_path')

    print()
    pprint(f"gdalbuildvrt {str(vrt)} {' '.join(dem_file_paths)}")
    gdal.BuildVRT(str(vrt), dem_file_paths)
    # subprocess.run(f"gdalbuildvrt {str(vrt)} {' '.join(dem_file_paths)}") #TODO: this line didn't work, check why.


# import re
# # Function to update the values
# def update_file_name(value):
#     # Match the pattern N followed by 3 digits and W or E followed by 3 digits
#     pattern = re.compile(r'N(\d{3})([WE]\d{3}_.+)')
#     match = pattern.match(value)
#     if match:
#         # Convert the latitude part to an integer to remove leading zeros, then format it back
#         new_value = f"N{int(match.group(1)):02d}{match.group(2)}"
#         return new_value
#     return value





def log_error_ids(hybas_id, dst_err_ids: str=f"outputs/error_ids.txt"):
    with open(dst_err_ids, "a") as log_file:
        log_file.write(f"{hybas_id}\n")


import multiprocessing

def run_with_timeout(func, args=(), kwargs={}, timeout=5):
    """ usage: result = run_with_timeout(long_running_function, timeout=3) """

    with multiprocessing.Manager() as manager:
        result = manager.list([None])  # Shared memory list to store result

        def target(result_list):
            result_list[0] = func(*args, **kwargs)

        process = multiprocessing.Process(target=target, args=(result,))
        process.start()
        process.join(timeout)

        if process.is_alive():
            process.terminate()  # Kill the process
            process.join()
            return None  # Timeout occurred

        return result[0]

if __name__ == "__main__":
    

    import os, time
    import numpy as np
    from tqdm import tqdm
    import rasterio
    from pathlib import Path 
    from shapely.geometry import GeometryCollection, box
    # from asf_tools.dem import prepare_dem_vrt

    import geopandas as gpd

    # # Set the timeout (in seconds)
    # timeout = 0.5 * 60 * 60  # 2 hours
    # # Set the signal handler for the alarm
    # signal.signal(signal.SIGALRM, handler)



    # Define a timeout handler
    def raise_timeout():
        raise TimeoutError("The operation took too long and was skipped.")

    timeout = threading.Timer(1*60*60, raise_timeout)  # 1-hour timeout
           
    # os.chdir("/home/jovyan/exchange/projects/HAND-from-FABDEM")
    print("Current Working Directory:", os.getcwd())
    
    

    # Italy, northern Algeria, Kenya, Uganda, South Africa / East Africa, Australia 
    country_name = "eu"
    query_id_by_country = False # True for querying hybas_id by country, False by HydroBASIN
    region = 'eu' # sa, as, af, eu: note, region and country_name need to be consistent
    level = 6

    acc_thresh = [100, 1000, 100000] # List of Integers, specify a list of accumulation thresholds
    # fabdem_path = Path("data/FABDEM/tiles")
    fabdem_path = Path(r"D:\puzh\HAND-from-FABDEM\outputs\strm_dem_tiles")

    save_dir = Path(f"outputs_{region}")
    save_dir.mkdir(exist_ok=True, parents=True)
    hand_path = save_dir / f"hand_uint16"
    hand_path.mkdir(exist_ok=True, parents=True)
    
    # TODO: change basin source!
    hydroBASIN = gpd.read_file(f"data/hydroBASIN/hybas_{region}_lev0{level}_v1c.zip")

    # basin level 6
    from constant import amenia_lv6 as missing_ids_lv6
    basin_lv6 = gpd.read_file(f"data/hydroBASIN/hybas_{region}_lev0{level}_v1c.zip")
    hydroBASIN = basin_lv6[basin_lv6.HYBAS_ID.isin(missing_ids_lv6)]
    print(hydroBASIN)

    if query_id_by_country: # query hybas_id by country
        print(f"query hybas_id by country: {country_name}.")
        from step1_download_fabdem_by_country import query_by_country
        _, hybas_ids = query_by_country(country_name=country_name)

        dst_err_ids=save_dir / f'{country_name}_error_ids.txt' # where to save error hybas_id
               
    else: # query hybas_id from HydroBASIN
        print(f"query hybas_id from HydroBASIN for region: {region}.")
        hybas_ids = hydroBASIN.HYBAS_ID.unique()

        dst_err_ids=save_dir / f'{region}_error_ids.txt' # where to save error hybas_id

    print('hybas_ids')
    # pprint(hybas_ids)
    print(f'{len(hybas_ids)} basins to be generted ...')
    print()

    # idx_stopped = list(hybas_ids).index(2050085710)
    # print(f"idx_stopped: {idx_stopped}")

    # hybas_ids = []
    # # for idx, hybas_id in enumerate(tqdm([2050014550, 6050029730])): # 2050014550, 6050029730
    for idx, hybas_id in enumerate(tqdm(hybas_ids)): # 6050068100, 6050000740
    # # for idx, hybas_id in enumerate(tqdm(hydroBASIN.HYBAS_ID.unique())): #  6050069460, 6050001940, 6050266740

        if (idx >= 0): # 333, 433 skiped

            print(f"idx: {idx}, hybas_id: {hybas_id}")

            basin = hydroBASIN[hydroBASIN.HYBAS_ID==hybas_id] # 6050069460

            print(f"=============================== idx: {idx}, hybas_id: {hybas_id} ===================================")
            print('basin SUB_AREA', basin.SUB_AREA)
            print('basin UP_AREA', basin.UP_AREA)

            basin_geo_buff = basin.geometry.to_crs("EPSG:3857").buffer(0.2).to_crs("EPSG:4326")
            basin_geo = GeometryCollection([basin_geo_buff])[0]
            basin_geo_union = basin_geo_buff.unary_union

            start_time = time.time()

            try:
                vrt_dir = save_dir / "vrt"
                vrt_dir.mkdir(exist_ok=True, parents=True)
                fabdem_vrt = vrt_dir / f'fabdem_basin{level}_id_{hybas_id}.vrt'
                prepare_fabdem_vrt(vrt=str(fabdem_vrt), geometry=basin_geo_union, dem='fabdem', fabdem_path=fabdem_path)

                from calculate import calculate_hand_for_basins
                hand_raster =  hand_path / f'hand_acc_thresh_basin{level}_id_{hybas_id}.tif'

                print("calculate_hand_for_basins ...")
                calculate_hand_for_basins(hand_raster, basin_geo, fabdem_vrt, acc_thresh=acc_thresh, hybas_id=hybas_id)

            except (np.core._exceptions._ArrayMemoryError, ValueError) as e:
                print(f"Exception message: {e}")
                log_error_ids(hybas_id, dst_err_ids)
            
            end_time = time.time()
            elapsed_time = end_time - start_time

            print(f'elapsed_time (minutes): {elapsed_time / 60 :.2f}')


        