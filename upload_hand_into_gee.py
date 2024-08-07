# This code should be run in python environment, requires earthengine-api, gcoud etc.
# Please contact PUZH@dhigroup.com if you encounter any problem with this script.

import zipfile, os
from pathlib import Path

import ee
ee.Initialize()


def upload_image_into_gee_from_gs(filename):
    ''' upload image into GEE from Google Cloud Storge'''

    asset_id = f'{eeImgCol}/{filename[:-4]}'
    # print(f'{index}: {asset_id}')
    os.system(f"earthengine upload image --force --asset_id={asset_id} --pyramiding_policy=sample --nodata_value=0 {gs_dir}/{filename}")


# Function to upload a GeoTIFF file to GEE and set properties
def upload_geotiff_with_properties(filepath):
    # Extract the filename without extension for asset name
    filename = os.path.basename(filepath).split('.')[0]
    
    # Set the asset ID (where the asset will be stored in your GEE account)
    asset_id = f'projects/global-wetland-watch/assets/features/hand/{filename}'
    
    # Define properties to set on the asset
    properties = {
        # 'source': 'My Data Source',
        'generated_date': '2024-08-07',
        'product': 'hand_100',
        'acc_thresh': 100,
        'dem': 'FABDEM',
        'basin_level': 5,
        'basin_id': filename.split("_")[-1]
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
    
    ''' batch upload local geotiffs to GEE '''
    # create an asset of ImageCollection in GEE, and bucket in GCP
    eeImgCol = 'projects/global-wetland-watch/assets/features/hand' # asset folder in GEE asset
    gs_dir = 'gs://hand_from_fabdem/hand_acc100_uint16' # Google Storage Folder

    # specify data folder
    data_dir = Path(r"C:\Users\puzh\Downloads\GWL_FCS30") # local ZIP data folder
    out_dir = Path(r"C:\DHI\HAND\hand_acc100_uint16") # extracted folder

    # # extract zip into the same folder
    if False: 
        for filename in os.listdir(data_dir):
            if filename.endswith('.zip'):
                print(filename)

                with zipfile.ZipFile(data_dir / filename,"r") as zip_ref:
                    zip_ref.extractall(out_dir)
                    
    # upload all geotiff into Google Cloud Storage (GS)
    # os.system(f"gsutil -m cp -r {out_dir} {gs_dir}/")

# batch upload from GS
fileList = [f for f in os.listdir(Path(out_dir)) if f.endswith('.tif')]
for filename in fileList:
    # upload_image_into_gee_from_gs(filename)
    print()
    print(f"------------------ {filename} ------------------")
    upload_geotiff_with_properties(f"{gs_dir}/hand_acc100_uint16/{filename}")