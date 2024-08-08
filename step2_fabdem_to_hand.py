# FABDEM: https://data.bris.ac.uk/data/dataset/s5hqmjcdj8yo2ibzi9b4ew3sn
# FABDEM in GEE: https://gee-community-catalog.org/projects/fabdem/

# Flow Accumulation Visualization: https://code.earthengine.google.com/eae949c6188239ea0108b9d61cddb9e3
# check failed hybas_id (from level-5 to level-6): https://code.earthengine.google.com/1a094d97538255a5039a6d36db002a07
# compare hand: https://code.earthengine.google.com/760177edebe0ba65bf6feb9220a886cb

"""Prepare a Copernicus GLO-30 DEM virtual raster (VRT) covering a given geometry"""
from pathlib import Path
from typing import Union

from osgeo import gdal, ogr
from shapely.geometry.base import BaseGeometry

from asf_tools import vector
from asf_tools.util import GDALConfigManager

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
        if isinstance(geometry, BaseGeometry):
            geometry = ogr.CreateGeometryFromWkb(geometry.wkb)

        min_lon, max_lon, _, _ = geometry.GetEnvelope()
        if min_lon < -160. and max_lon > 160.:
            raise ValueError(f'asf_tools does not currently support geometries that cross the antimeridian: {geometry}')

        tile_features = vector.get_features(DEM_GEOJSON)
        if not vector.get_property_values_for_intersecting_features(geometry, tile_features):
            raise ValueError(f'Copernicus GLO-30 DEM does not intersect this geometry: {geometry}')


        if 'fabdem' == dem:
          dem_file_names = vector.intersecting_feature_properties(geometry, tile_features, 'file_name')

          # fabdem_path = Path("C:/DHI/HAND/DEM/N00W080-N10W070_FABDEM_V1-2")
          fabdem_path = Path(fabdem_path)
          dem_file_paths = [fabdem_path / filename for filename in dem_file_names]

        else:
            dem_file_paths = vector.intersecting_feature_properties(geometry, tile_features, 'file_path')

        gdal.BuildVRT(str(vrt), dem_file_paths)


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

def log_error_ids(hybas_id):
    with open("outputs/error_ids.txt", "a") as log_file:
        log_file.write(f"Failed to process file: {hybas_id}\n")




if __name__ == "__main__":
    

    import os, time
    import numpy as np
    from tqdm import tqdm
    import rasterio
    from pathlib import Path 
    from shapely.geometry import GeometryCollection, box
    # from asf_tools.dem import prepare_dem_vrt

    import geopandas as gpd

    acc_thresh = 100 # accumulation threshold
    fabdem_path = Path("data/FABDEM/sa")

    hand_path = Path(f"outputs/hand_acc{acc_thresh}")
    hand_path.mkdir(exist_ok=True, parents=True)
    
    # Italy, northern Algeria, Kenya, Uganda, South Africa, Australia 
    hydroBASIN = gpd.read_file("data/hydroBASIN/hybas_sa_lev05_v1c.zip")

    from constant import missing_ids
    for idx, hybas_id in enumerate(tqdm(missing_ids)): # 6050068100, 6050000740
    # for idx, hybas_id in enumerate(tqdm(hydroBASIN.HYBAS_ID.unique())): #  6050069460, 6050001940, 6050266740

        # if (idx >= 288) and (idx < 388): # 288 -> 387

        basin = hydroBASIN[hydroBASIN.HYBAS_ID==hybas_id] # 6050069460

        print(f"=============================== idx: {idx}, hybas_id: {hybas_id} ===================================")
        print('basin SUB_AREA', basin.SUB_AREA)
        print('basin UP_AREA', basin.UP_AREA)

        basin_geo = GeometryCollection([basin.geometry])[0]

        start_time = time.time()

        fabdem_vrt = Path("outputs") / 'vrt' / f'fabdem_basin5_id_{hybas_id}.vrt'
        prepare_fabdem_vrt(vrt=fabdem_vrt, geometry=basin_geo, dem='fabdem', fabdem_path=fabdem_path)

        from calculate import calculate_hand_for_basins
        hand_raster =  hand_path / f'hand_{acc_thresh}_basin5_id_{hybas_id}.tif'

        try:
            calculate_hand_for_basins(hand_raster, basin_geo, fabdem_vrt, acc_thresh=acc_thresh)
        except np.core._exceptions._ArrayMemoryError as e:
            print(f"Exception message: {e}")
            log_error_ids(hybas_id)

        end_time = time.time()
        elapsed_time = end_time - start_time

        print(f'elapsed_time (minutes): {elapsed_time / 60 :.2f}')