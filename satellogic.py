from pyproj import Transformer
from shapely.geometry import box, Polygon


def intersection_flag(sample):

  bounds = sample['metadata']['bounds']
  crs = sample['metadata']['crs'][0]

  # Define the transformer
  transformer = Transformer.from_crs(crs, "EPSG:4326")

  # # Define the coordinates in EPSG:32723
  # geometry = [[178191.0, 8248444.0, 178575.0, 8248828.0]]

  # Convert the coordinates
  converted_geometry = [
      [
          transformer.transform(y_min, x_min),
          transformer.transform(y_max, x_max)
      ]
      for x_min, y_min, x_max, y_max in bounds
  ]

  # Extract the coordinates
  (min_lon, min_lat), (max_lon, max_lat) = converted_geometry[0]

  # Create the bounding box
  bbox = [min_lon, min_lat, max_lon, max_lat]

  # print("Converted Geometry:", converted_geometry)

  # Create a Shapely box from the bbox
  bbox_geom = box(bbox[0], bbox[1], bbox[2], bbox[3])

  roi_coords = [
            [(148.2528281907821, -42.109682876990234),
            (148.25557477281336, -41.986799441913384),
            (148.2198692064071, -41.94238064552847),
            (148.08460004136805, -42.01945556720649),
            (148.0708671312118, -42.074524769176044),
            (148.08460004136805, -42.09032285173902)],

          [(147.68428743253466, -42.98355527825432),
            (147.6709836758208, -42.997806817494244),
            (147.68102586637255, -43.003205215917625),
            (147.69596040616747, -42.99071285199332),
            (147.6973336971831, -42.979725006156045),
            (147.68360078702685, -42.97357095463549),],

          [
              [
                -97.67718258896704,
                33.322166721479185
              ],
              [
                -97.67648642932272,
                33.322166721479185
              ],
              [
                -97.67648642932272,
                33.322751164251514
              ],
              [
                -97.67718258896704,
                33.322751164251514
              ],
              [
                -97.67718258896704,
                33.322166721479185
              ]] 
        ]

  # Create Shapely Polygon objects
  roi = [Polygon(coords) for coords in roi_coords]

  # Check if the bbox intersects with the other geometry
  intersects = bbox_geom.intersects(roi)
  flag = intersects[0] or intersects[-1]
  return flag


# flag = intersection_flag(images)
# flag




import numpy as np
import earthview as ev
from tqdm import tqdm

token = "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"
data = ev.load_dataset("satellogic", shards=[10])  # shard is optional
sample = next(iter(data))

print(sample.keys())
print(np.array(sample['rgb']).shape)      # RGB Data
print(np.array(sample['1m']).shape)       # NIR Data


# neon = ev.load_dataset("neon", shards=[100])  # shard is optional

intersecting_samples = []
for idx, sample in enumerate(tqdm(data)):
  try:
    flag = intersection_flag(sample) 
    if flag: 
      print(idx)
      intersecting_samples.append(sample)
  except:
    pass

