import ee

# Initialize the Earth Engine library
ee.Initialize()

# Specify your folder path
folder = 'projects/global-wetland-watch/assets/features/flow_accumulation'

# Get a list of assets in the folder
asset_list = ee.data.listAssets({'parent': folder})

# hand_100_basin5_id_2050014550
# hand_1000_basin5_id_2050014550
# hand_100000_basin5_id_2050014550

ids_to_del = [
    1050761260, # start
    1050761270,
    1050025000,
    1050024880,
    1050024870, 
    1050024320, # end
]
# Delete each asset
for asset in asset_list['assets']:
    id = asset['name'].split("_")[-1]
    # print(id)
    if ('V0' in asset['name']) or (int(id) in ids_to_del):
      ee.data.deleteAsset(asset['name'])
      print(f"Deleted: {asset['name']}")
