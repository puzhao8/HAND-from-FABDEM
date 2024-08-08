
import os
import numpy as np
import rasterio
from tqdm import tqdm
from rasterio import Affine
from rasterio.enums import Resampling


def convert_geotiff_to_uint16(input_file, output_file, nodata_value = 65535):
    with rasterio.open(input_file) as src:
        # Read the data
        data = src.read(1).astype(np.float32)  # Assuming single-band GeoTIFF
        
        # Normalize and scale the data to uint16
        # data = data * 10
        data[np.isnan(data)] = nodata_value
        data_uint16 = data.astype(np.uint16)
        # data_uint16[np.isnan(data_uint16)] = 10000
        
        # Get the metadata and update it for uint16
        meta = src.meta
        # meta.update(dtype=rasterio.uint16, count=1, compress='lzw')
        meta.update(dtype=rasterio.uint16, count=1, nodata=nodata_value, compress='lzw')

        # Write the data to the new file
        with rasterio.open(output_file, 'w', **meta) as dst:
            dst.write(data_uint16, 1)



if __name__ == "__main__":


    from pathlib import Path
    current_directory = os.getcwd()

    in_folder = Path("outputs/flow_acc")
    out_folder = Path("outputs/flow_acc_uint16")
    out_folder.mkdir(exist_ok=True, parents=True)
    # print(os.listdir(in_folder))

    fileList = [f for f in os.listdir(in_folder) if f.startswith("flow_acc") and f.endswith(".tif")]
    # print(fileList)

    for filename in tqdm(fileList):
        input_file = in_folder / filename
        output_file = out_folder / filename

        if not output_file.exists():
            print(filename)
            convert_geotiff_to_uint16(input_file, output_file)


