
# HAND based on GLO-30: https://github.com/ASFHyP3/OpenData/tree/main/glo-30-hand

# dem_tile = '/vsicurl/https://copernicus-dem-30m.s3.amazonaws.com/Copernicus_DSM_COG_10_N41_00_W088_00_DEM/Copernicus_DSM_COG_10_N41_00_W088_00_DEM.tif'
import os
import numpy as np
import rasterio
from pathlib import Path 
from shapely.geometry import GeometryCollection, box
from asf_tools.dem import prepare_dem_vrt

accumulation_threshold = 1000
dem_dir = Path("C:/DHI/HAND/DEM/FABDEM")
print(f"ACC: {accumulation_threshold}")

out_dir = Path(f"C:/DHI/HAND/outputs_ACC_{accumulation_threshold}_test") 
out_dir.mkdir(exist_ok=True, parents=True)

tmp_dir =  out_dir / "tmp"
tmp_dir.mkdir(exist_ok=True, parents=True)

# hydroBASIN
import geopandas as gpd
hydroBASIN = gpd.read_file("hydroBASIN/hybas_sa_lev05_v1c\hybas_sa_lev05_v1c.shp")

fileList = os.listdir(dem_dir)
for filename in fileList[:1]:
  filename = filename[:-4]
  print(filename)

  dem_tile = dem_dir / f"{filename}.tif"

  with rasterio.open(dem_tile) as dem:
          dem_bounds = dem.bounds
          dem_meta = dem.meta

  dem_geometry = box(*dem_bounds)
  aoi_buffered = GeometryCollection([dem_geometry.buffer(0.5)])

  basins =  hydroBASIN[hydroBASIN.geometry.within(aoi_buffered)]


  buffered_dem_vrt = tmp_dir / dem_tile.name.replace('.tif', '_buffered.vrt')
  prepare_dem_vrt(buffered_dem_vrt, aoi_buffered)

  hand_raster_buffered = tmp_dir / f'HAND_ACC{accumulation_threshold}_buffered.tif'

  from calculate import calculate_hand_for_basins
  calculate_hand_for_basins(hand_raster_buffered, aoi_buffered, buffered_dem_vrt, acc_thresh=accumulation_threshold)

  # Crop out buffered HAND to the original DEM tile size
  with rasterio.open(hand_raster_buffered) as sds:
      window = rasterio.windows.from_bounds(*dem_bounds, sds.transform)
      out_pixels = sds.read(
          1, window=window, out_shape=(dem_meta['height'], dem_meta['width']),
          resampling=rasterio.enums.Resampling.bilinear
      )


  # # Mask the ocean pixels as identified in the WBM auxiliary DEM files
  # wmb_tile = dem_tile.replace('DEM/Copernicus', 'DEM/AUXFILES/Copernicus')
  # wmb_tile = wmb_tile.replace('_DEM.tif', f'_WBM.tif')

  # with rasterio.open(wmb_tile) as wbm:
  #     wbm_pixels = wbm.read(1)

  # out_pixels = np.ma.masked_where(wbm_pixels == 1, out_pixels)


  # Write out the final HAND tile
  from calculate import write_cog

  hand_raster = out_dir/ f'{filename}_HAND_ACC{accumulation_threshold}.tif'
  write_cog(hand_raster, out_pixels, transform=dem.transform.to_gdal(), epsg_code=dem.crs.to_epsg())