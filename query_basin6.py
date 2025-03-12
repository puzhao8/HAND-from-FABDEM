
#%%

import geopandas as gpd

region = "eu"
basin_lv5 = gpd.read_file(f"data/hydroBASIN/hybas_{region}_lev05_v1c.zip")
basin_lv6 = gpd.read_file(f"data/hydroBASIN/hybas_{region}_lev06_v1c.zip")

level5_ids_of_interest = [2050014550, 2050059510]
level5_basins_of_interest = basin_lv5[basin_lv5['HYBAS_ID'].isin(level5_ids_of_interest)]

level6_basins_of_interest = gpd.sjoin(basin_lv6, level5_basins_of_interest, how='inner', predicate='within')
# level6_basins_of_interest = basin_lv6[basin_lv6.intersects(level5_basins_of_interest)]
level6_basins_of_interest