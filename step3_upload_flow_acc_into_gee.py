# This code should be run in python environment, requires earthengine-api, gcoud etc.
# Please contact PUZH@dhigroup.com if you encounter any problem with this script.

import time
import zipfile, os
from pathlib import Path

import ee
ee.Initialize()


# def upload_image_into_gee_from_gs(filename):
#     ''' upload image into GEE from Google Cloud Storge'''

#     asset_id = f'{eeImgCol}/{filename[:-4]}'
#     # print(f'{index}: {asset_id}')
#     os.system(f"earthengine upload image --force --asset_id={asset_id} --pyramiding_policy=sample --nodata_value=0 {gs_dir}/{filename}")


# Function to upload a GeoTIFF file to GEE and set properties
def upload_geotiff_with_properties(filepath, acc_thresh=1000, basin_level=5):
    # Extract the filename without extension for asset name
    filename = os.path.basename(filepath).split('.')[0]
    
    # Set the asset ID (where the asset will be stored in your GEE account)
    asset_id = f"{eeImgCol}/{filename}"
    
    cur_time = int(time.time() * 1000)
    # Define properties to set on the asset
    properties = {
        # 'source': 'My Data Source',
        'generated_time': cur_time,
        'product': f'flow_accumulation',
        'dem': 'FABDEM',
        'basin_level': basin_level,
        'basin_id': filename.split("_")[-1],
        'time_start': cur_time,
        'time_end': cur_time,
    }
    
    # Create an ingestion request with properties
    ingestion_request = {
        'id': asset_id,
        # 'type': 'Image',
        'properties': properties,
        # 'sourceFiles': [filepath],
        # 'sources': [{'primaryPath': filepath, 'additionalPaths': []}]
        # 'sources': [filepath]
        'tilesets': [{
            'sources': [{
                'uris': [filepath]
            }]
        }]
    }

    task_id = ee.data.newTaskId()[0]
    
    # Start the ingestion task
    task = ee.data.startIngestion(
        # ee.data.getAssetIdFromPath(asset_id),
        task_id,
        ingestion_request
    )

    # task = ee.batch.Task.ingest(asset_id, ingestion_request)
    
    print(f'Uploading {filepath} as {asset_id} with properties: ')
    print(properties)
    return task


if __name__ == "__main__":
    

    # specify data folder
    basin_level = 6
    acc_thresh = 100

    folder = f"flow_acc"
    data_dir = Path(f"C:/DHI/HAND-from-FABDEM/outputs/{folder}") # extracted folder
    print(data_dir)

    ''' batch upload local geotiffs to GEE '''
    # create an asset of ImageCollection in GEE, and bucket in GCP
    eeImgCol = 'projects/global-wetland-watch/assets/features/flow_accumulation' # asset folder in GEE asset
    gs_dir = 'gs://hand_from_fabdem' # Google Storage Folder


    if False:
        os.system(f"gsutil -m cp -r {data_dir} {gs_dir}/")

    # batch upload from GS
    fileList = [f for f in os.listdir(Path(data_dir)) if f.startswith('flow_acc') and f.endswith('.tif')]
    for filename in fileList:
        # upload_image_into_gee_from_gs(filename)
        print()
        print(f"------------------ {filename} ------------------")

        id = filename.split('_id_')[-1][:-4]
        if id.startswith('2050'):
            print(f"id: {id}, basin: 5")
            upload_geotiff_with_properties(f"{gs_dir}/{folder}/{filename}", basin_level=5)
        else:
            upload_geotiff_with_properties(f"{gs_dir}/{folder}/{filename}", basin_level=basin_level)