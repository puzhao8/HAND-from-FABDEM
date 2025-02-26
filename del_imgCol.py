
import os
import time 
import datetime as dt
from datetime import datetime, timedelta
import subprocess
import ee 
ee.Initialize()

# feature = "projects/geo4gras/assets/NbS"
# for imgCol in ['swe']:
from prettyprinter import pprint
from constant import italy_ids_lv5

eeImgCol = "projects/global-wetland-watch/assets/features/hand"

def get_asset_ids(eeImgCol):
    response = subprocess.getstatusoutput(f"earthengine ls {eeImgCol}")
    asset_list = response[1].replace("projects/earthengine-legacy/assets/", "").split("\n")
    return asset_list

asset_list = get_asset_ids(eeImgCol)
print(f"before deleting: {len(asset_list)}")

basin_ids_to_del = []
if len(asset_list) > 0:
    for asset_id in asset_list:
                
        filename = os.path.split(asset_id)[-1]
        basin_id = int(filename.split("id_")[-1])
        if filename.startswith('hand') and (basin_id in italy_ids_lv5) and (basin_id!=2050014550): 
            print(f"{filename}: {asset_id}")
            os.system(f"earthengine rm {asset_id}")

            basin_ids_to_del.append(basin_id)

asset_list = get_asset_ids(eeImgCol)
pprint(set(basin_ids_to_del))
print(f"after deleting: {len(asset_list)}")