# FABDEM: https://data.bris.ac.uk/data/dataset/s5hqmjcdj8yo2ibzi9b4ew3sn
# FABDEM in GEE: https://gee-community-catalog.org/projects/fabdem/

# Flow Accumulation Visualization: https://code.earthengine.google.com/eae949c6188239ea0108b9d61cddb9e3
# check failed hybas_id (from level-5 to level-6): https://code.earthengine.google.com/1a094d97538255a5039a6d36db002a07
# compare hand: https://code.earthengine.google.com/760177edebe0ba65bf6feb9220a886cb

"""Prepare a Copernicus GLO-30 DEM virtual raster (VRT) covering a given geometry"""
import geopandas as gpd
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


def prepare_fabdem_vrt(vrt: Union[str, Path], geometry: Union[ogr.Geometry, BaseGeometry], dem='fabdem', fabdem_path='DEM/FABDEM'):
    """Create a DEM mosaic VRT covering a given geometry

    The DEM mosaic is assembled from the Copernicus GLO-30 DEM tiles that intersect the geometry.

    Note: `asf_tools` does not currently support geometries that cross the antimeridian.

    Args:
        vrt: Path for the output VRT file
        geometry: Geometry in EPSG:4326 (lon/lat) projection for which to prepare a DEM mosaic

    """

    if 'fabdem' == dem:
        DEM_GEOJSON = 'data/FABDEM_v1-2_tiles.geojson'

    with GDALConfigManager(GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR'):
        # if isinstance(geometry, BaseGeometry):
        #     geometry = ogr.CreateGeometryFromWkb(geometry.wkb)

        # min_lon, max_lon, _, _ = geometry.GetEnvelope()
        # if min_lon < -160. and max_lon > 160.:
        #     raise ValueError(f'asf_tools does not currently support geometries that cross the antimeridian: {geometry}')

        # tile_features = vector.get_features(DEM_GEOJSON)
        # if not vector.get_property_values_for_intersecting_features(geometry, tile_features):
        #     raise ValueError(f'Copernicus GLO-30 DEM does not intersect this geometry: {geometry}')


        # if 'fabdem' == dem:
        #   dem_file_names = vector.intersecting_feature_properties(geometry, tile_features, 'file_name')

        #   # fabdem_path = Path("C:/DHI/HAND/DEM/N00W080-N10W070_FABDEM_V1-2")
        #   fabdem_path = Path(fabdem_path)
        #   dem_file_paths = [str(fabdem_path / filename) for filename in dem_file_names]

        if 'dtm_vhr' == dem:

            tiles = gpd.read_file("data/DKN_1km_euref89.zip")
            tiles_filtered = tiles[tiles.intersects(geometry)]
            tileList = sorted(list(tiles_filtered.KN1kmDK.unique()))

            dem_file_paths = [f"C:/Users/puzh/Downloads/Thy_DTM/DTM_{tile_name.split('_')[1][:3]}_{tile_name.split('_')[-1][:2]}_TIF_UTM32-ETRS89/DTM_{tile_name}.tif" for tile_name in tileList]

        # else:
        #     dem_file_paths = vector.intersecting_feature_properties(geometry, tile_features, 'file_path')

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


# Define a timeout handler
def handler(signum, frame):
    raise TimeoutError("The operation took too long and was skipped.")


def log_error_ids(hybas_id, dst_err_ids: str="outputs/error_ids.txt"):
    with open(dst_err_ids, "a") as log_file:
        log_file.write(f"{hybas_id}\n")



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
    # timeout = 2 * 60 * 60  # 2 hours
    # # Set the signal handler for the alarm
    # signal.signal(signal.SIGINT, handler)
                       
    # os.chdir("/home/jovyan/exchange/projects/HAND-from-FABDEM")
    print("Current Working Directory:", os.getcwd())
    
    Path("outputs").mkdir(exist_ok=True, parents=True)

    # Italy, northern Algeria, Kenya, Uganda, South Africa / East Africa, Australia 
    region = 'DK' # sa, af, eu: note, region and country_name need to be consistent

    acc_thresh = [100, 1000, 100000] # List of Integers, specify a list of accumulation thresholds
    # fabdem_path = Path("data/FABDEM/tiles")
    fabdem_path = Path("C:/Users/puzh/Downloads/Thy_DTM")

    hand_path = Path(f"outputs/hand_uint16_dtm_vhr")
    hand_path.mkdir(exist_ok=True, parents=True)
    
    dst_err_ids=f'outputs/{region}_error_ids.txt' # where to save error hybas_id

    
    basins = gpd.read_file("data/Thy_basins.zip")
    basins = basins.to_crs(epsg=25832)

    hybas_ids = [2120024880] # 2120024880, 2120024900, 2120024910, 2121076350, 2120024920

    print('hybas_ids')
    pprint(hybas_ids)
    print(f'{len(hybas_ids)} basins to be generted ...')
    print()
    
    # for idx, hybas_id in enumerate(tqdm([2050014550, 6050029730])): # 2050014550, 6050029730
    for idx, hybas_id in enumerate(tqdm(hybas_ids)): # 6050068100, 6050000740
    # for idx, hybas_id in enumerate(tqdm(hydroBASIN.HYBAS_ID.unique())): #  6050069460, 6050001940, 6050266740

        if (idx >= 0): # 771

            print(idx, hybas_id)
            basin = basins[basins['HYBAS_ID']==hybas_id]

            print(f"=============================== idx: {idx}, hybas_id: {hybas_id} ===================================")
            print('basin SUB_AREA', basin.SUB_AREA)
            print('basin UP_AREA', basin.UP_AREA)

            basin_geo = GeometryCollection([basin.geometry])[0]
            basin_geo_buff = basin_geo.buffer(0.5)

            start_time = time.time()
            
            Path("outputs/vrt").mkdir(exist_ok=True, parents=True)
            fabdem_vrt = Path("outputs/vrt") / f'fabdem_basin5_id_{hybas_id}.vrt'
            prepare_fabdem_vrt(vrt=str(fabdem_vrt), geometry=basin_geo_buff, dem='dtm_vhr', fabdem_path=fabdem_path)

            from calculate import calculate_hand_for_basins
            hand_raster =  hand_path / f'hand_acc_thresh_basin5_id_{hybas_id}.tif'

            try:
                print("calculate_hand_for_basins ...")
                # signal.alarm(timeout)  # Start the alarm
                calculate_hand_for_basins(hand_raster, basin_geo, fabdem_vrt, acc_thresh=acc_thresh, hybas_id=hybas_id)
                # signal.alarm(0) # Disable the alarm if the function completes within the timeout

            # except TimeoutError as e:
            #     print(f"Exception message: {e}")
            #     log_error_ids(hybas_id, dst_err_ids)
            
            except np.core._exceptions._ArrayMemoryError as e:
                print(f"Exception message: {e}")
                log_error_ids(hybas_id, dst_err_ids)

            end_time = time.time()
            elapsed_time = end_time - start_time

            print(f'elapsed_time (minutes): {elapsed_time / 60 :.2f}')