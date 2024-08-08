
# hydroSAR: https://github.com/HydroSAR/HydroSAR/blob/develop/src/hydrosar/hand/calculate.py

"""Calculate Height Above Nearest Drainage (HAND) from the Copernicus GLO-30 DEM"""
import argparse
import logging
import os, sys
import warnings
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional, Union

import astropy.convolution
import fiona
import numpy as np
import rasterio.crs
import rasterio.mask
from asf_tools.dem import prepare_dem_vrt
# from asf_tools.raster import write_cog
from pysheds.sgrid import sGrid
from shapely.geometry import GeometryCollection, shape

log = logging.getLogger(__name__)


from osgeo import gdal
from asf_tools.util import epsg_to_wkt
from typing import List, Literal, Union

def write_cog(file_name: Union[str, Path], data: np.ndarray, transform: List[float], epsg_code: int,
              dtype=gdal.GDT_Float32, nodata_value=None):
    """Creates a Cloud Optimized GeoTIFF

    Args:
        file_name: The output file name
        data: The raster data
        transform: The geotransform for the output GeoTIFF
        epsg_code: The integer EPSG code for the output GeoTIFF projection
        dtype: The pixel data type for the output GeoTIFF
        nodata_value: The NODATA value for the output Geotiff

    Returns:
        file_name: The output file name
    """
    log.info(f'Creating {file_name}')

    with NamedTemporaryFile(delete=False) as temp_file:
      driver = gdal.GetDriverByName('GTiff')
      temp_geotiff = driver.Create(temp_file.name, data.shape[1], data.shape[0], 1, dtype)
      temp_geotiff.GetRasterBand(1).WriteArray(data)
      if nodata_value is not None:
          temp_geotiff.GetRasterBand(1).SetNoDataValue(nodata_value)
      temp_geotiff.SetGeoTransform(transform)
      temp_geotiff.SetProjection(epsg_to_wkt(epsg_code))

      driver = gdal.GetDriverByName('COG')
      options = ['COMPRESS=LZW', 'OVERVIEW_RESAMPLING=AVERAGE', 'NUM_THREADS=ALL_CPUS', 'BIGTIFF=YES']
      driver.CreateCopy(str(file_name), temp_geotiff, options=options)

      del temp_geotiff  # How to close w/ gdal

    return file_name

def fill_nan(array: np.ndarray) -> np.ndarray:
    """Replace NaNs with values interpolated from their neighbors

    Replace NaNs with values interpolated from their neighbors using a 2D Gaussian
    kernel, see: https://docs.astropy.org/en/stable/convolution/#using-astropy-s-convolution-to-replace-bad-data
    """
    kernel = astropy.convolution.Gaussian2DKernel(x_stddev=3)  # kernel x_size=8*stddev
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        while np.any(np.isnan(array)):
            array = astropy.convolution.interpolate_replace_nans(
                array, kernel, convolve=astropy.convolution.convolve
            )

    return array

def fill_hand(hand: np.ndarray, dem: np.ndarray):
    """Replace NaNs in a HAND array with values interpolated from their neighbor's HOND

    Replace NaNs in a HAND array with values interpolated from their neighbor's HOND (height of nearest drainage)
    using a 2D Gaussian kernel. Here, HOND is defined as the DEM value less the HAND value. For the kernel, see:
    https://docs.astropy.org/en/stable/convolution/#using-astropy-s-convolution-to-replace-bad-data
    """
    hond = dem - hand
    hond = fill_nan(hond)

    hand_mask = np.isnan(hand)
    hand[hand_mask] = dem[hand_mask] - hond[hand_mask]
    hand[hand < 0] = 0

    return hand

def calculate_hand(dem_array, dem_affine: rasterio.Affine, dem_crs: rasterio.crs.CRS, basin_mask,
                   acc_thresh: Optional[int] = 100):
    """Calculate the Height Above Nearest Drainage (HAND)

     Calculate the Height Above Nearest Drainage (HAND) using pySHEDS library. Because HAND
     is tied to watershed boundaries (hydrobasins), clipped/cut basins will produce weird edge
     effects, and incomplete basins should be masked out. For watershed boundaries,
     see: https://www.hydrosheds.org/page/hydrobasins

     This involves:
        * Filling pits (single-cells lower than their surrounding neighbors)
            in the Digital Elevation Model (DEM)
        * Filling depressions (regions of cells lower than their surrounding neighbors)
            in the Digital Elevation Model (DEM)
        * Resolving un-drainable flats
        * Determining the flow direction using the ESRI D8 routing scheme
        * Determining flow accumulation (number of upstream cells)
        * Creating a drainage mask using the accumulation threshold `acc_thresh`
        * Calculating HAND

    In the HAND calculation, NaNs inside the basin filled using `fill_hand`

    Args:
        dem_array: DEM to calculate HAND for
        dem_crs: DEM Coordinate Reference System (CRS)
        dem_affine: DEM Affine geotransform
        basin_mask: Array of booleans indicating wither an element should be masked out (à la Numpy Masked Arrays:
            https://numpy.org/doc/stable/reference/maskedarray.generic.html#what-is-a-masked-array)
        acc_thresh: Accumulation threshold for determining the drainage mask.
            If `None`, the mean accumulation value is used
    """
    nodata_fill_value = np.finfo(float).eps
    # with NamedTemporaryFile() as temp_file:
    #     write_cog(temp_file.name, dem_array,
    #               transform=dem_affine.to_gdal(), epsg_code=dem_crs.to_epsg(),
    #               # Prevents PySheds from assuming using zero as the nodata value
    #               nodata_value=nodata_fill_value)

    #     # From PySheds; see example usage: http://mattbartos.com/pysheds/
    #     grid = sGrid.from_raster(str(temp_file.name))
    #     dem = grid.read_raster(str(temp_file.name))

    out_path = Path("outputs/tmp_dir")
    out_name = str(out_path / "fabdem.tif")
    write_cog(out_name, dem_array,
                  transform=dem_affine.to_gdal(), epsg_code=dem_crs.to_epsg(),
                  # Prevents PySheds from assuming using zero as the nodata value
                  nodata_value=nodata_fill_value)

    # From PySheds; see example usage: http://mattbartos.com/pysheds/
    grid = sGrid.from_raster(out_name)
    dem = grid.read_raster(out_name)

    log.info('Fill pits in DEM')
    pit_filled_dem = grid.fill_pits(dem)

    log.info('Filling depressions')
    flooded_dem = grid.fill_depressions(pit_filled_dem)
    del pit_filled_dem

    log.info('Resolving flats')
    inflated_dem = grid.resolve_flats(flooded_dem)
    del flooded_dem

    log.info('Obtaining flow direction')
    flow_dir = grid.flowdir(inflated_dem, apply_mask=True)

    log.info('Calculating flow accumulation')
    acc = grid.accumulation(flow_dir)

    if acc_thresh is None:
        acc_thresh = acc.mean()

    log.info(f'Calculating HAND using accumulation threshold of {acc_thresh}')
    hand = grid.compute_hand(flow_dir, inflated_dem, acc > acc_thresh, inplace=False)

    # write acc raster
    acc[basin_mask] = np.nan

    if np.isnan(hand).any():
        log.info('Filling NaNs in the HAND')
        # mask outside of basin with a not-NaN value to prevent NaN-filling outside of basin (optimization)
        hand[basin_mask] = nodata_fill_value
        hand = fill_hand(hand, dem_array)

    # # TODO: rescale hand by 10 to save space
    # hand = hand * 10
    # hand = hand.astype(np.uint16)
    # hand[basin_mask] = 65535

    # # set pixels outside of basin to nodata
    hand[basin_mask] = np.nan

    # TODO: also mask ocean pixels here?

    return hand, acc

def to_uint16(data, nodata_value=65535):
    # convert datatype from float32 into uint16
    data[np.isnan(data)] = nodata_value
    return data.astype(np.uint16)

def calculate_hand_for_basins(out_raster:  Union[str, Path], geometries: GeometryCollection,
                              dem_file: Union[str, Path], acc_thresh: Optional[int] = 100):
    """Calculate the Height Above Nearest Drainage (HAND) for watershed boundaries (hydrobasins).

    For watershed boundaries, see: https://www.hydrosheds.org/page/hydrobasins

    Args:
        out_raster: HAND GeoTIFF to create
        geometries: watershed boundary (hydrobasin) polygons to calculate HAND over
        dem_file: DEM raster covering (containing) `geometries`
        acc_thresh: Accumulation threshold for determining the drainage mask.
            If `None`, the mean accumulation value is used
    """

    nodata_value = 65535
    with rasterio.open(dem_file) as src:
        basin_mask, basin_affine_tf, basin_window = rasterio.mask.raster_geometry_mask(
            src, geometries.geoms, all_touched=True, crop=True, pad=True, pad_width=1
        )
        basin_array = src.read(1, window=basin_window)

        hand, acc = calculate_hand(basin_array, basin_affine_tf, src.crs, basin_mask, acc_thresh=acc_thresh)

        # TODO: Are these lines necessary ?!! Just rescale here?
        # convert datatype
        hand = to_uint16(hand * 10, nodata_value=nodata_value) # rescaled by 10
        flow_acc = to_uint16(acc, nodata_value=nodata_value) 

        # write hand, note NaN is not compatible with uint16 data type.
        write_cog(
            out_raster, hand, transform=basin_affine_tf.to_gdal(), epsg_code=src.crs.to_epsg(), nodata_value=nodata_value, dtype=gdal.GDT_UInt16) # np.nan

        # write accumlation if not exists
        filename = os.path.basename(out_raster) # hand_[100/1000]_basin5_id_6050942390.tif
        flow_acc_url = Path(f"outputs/flow_acc/flow_acc_basin{filename.split('basin')[-1]}") # flow_acc_basin5_id_6050942390.tif
        if not flow_acc_url.exists():
            write_cog(flow_acc_url, flow_acc, transform=basin_affine_tf.to_gdal(), epsg_code=src.crs.to_epsg(), nodata_value=nodata_value, dtype=gdal.GDT_UInt16)
        