
#%%

import geopandas as gpd
tiles = gpd.read_file("data/DKN_1km_euref89.zip")
basins = gpd.read_file("data/Thy_basins.zip")
basins = basins.to_crs(epsg=25832)

basin_1 = basins[basins['HYBAS_ID']==2120024920].geometry.iloc[0]
tiles_filtered = tiles[tiles.intersects(basin_1)]
tileList = sorted(list(tiles_filtered.KN1kmDK.unique()))

urlList = [f"DTM_{tile_name.split('_')[1][:3]}_{tile_name.split('_')[-1][:2]}_TIF_UTM32-ETR89/DTM_{tile_name}.tif" for tile_name in tileList]
