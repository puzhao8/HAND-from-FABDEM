
import os
import time 
import datetime as dt
from datetime import datetime, timedelta
import subprocess
import ee 
ee.Initialize()

# feature = "projects/geo4gras/assets/NbS"
# for imgCol in ['swe']:

eeImgCol = "projects/global-wetland-watch/assets/features/hand"

def get_asset_ids(eeImgCol):
    response = subprocess.getstatusoutput(f"earthengine ls {eeImgCol}")
    asset_list = response[1].replace("projects/earthengine-legacy/assets/", "").split("\n")
    return asset_list

asset_list = get_asset_ids(eeImgCol)
print(f"before deleting: {len(asset_list)}")
if len(asset_list) > 0:
    for asset_id in asset_list:
                
        filename = os.path.split(asset_id)[-1]
        if filename.startswith('flow_acc'): 
            print(f"{filename}: {asset_id}")
            os.system(f"earthengine rm {asset_id}")

asset_list = get_asset_ids(eeImgCol)
print(f"after deleting: {len(asset_list)}")