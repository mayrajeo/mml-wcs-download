import requests
import json
import geopandas as gpd
from pathlib import Path
from fastcore.script import *
from shapely.geometry import shape
from tqdm import tqdm

"""
Scrape aerial image layers from NLS aerial campaign map
"""


def make_url(layer_id:str, bbox:list):
    route_url = 'https://hkp.maanmittauslaitos.fi/hkp/action?action_route=GetWFSFeatures&'
    layer_info = f'id={layer_id}&'
    crs_info = 'srs=EPSG%3A3067&'
    bbox = f'bbox={bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}'
    return f'{route_url}{layer_info}{crs_info}{bbox}'

@call_parse
def get_time_layers(
    layer_ids:Path, # json file that has the mapping for layer ids and names
    outpath:Path, # Directory to save the resulting layers
):
    
    bounds = [61682,6605800,733000,7776500]
    
    with open(layer_ids) as f: layers = json.load(f)

    for user_layer, layer_name in tqdm(layers.items()):
        request_url = make_url(user_layer, bounds)
        for n in range(5):
            try:
                r = requests.get(request_url).json()
            except Exception as e:
                if n < 4:
                    print(f'{request_url} failed, retry {n}')
                else:
                    print(f'Failed to access {request_url}')
                    continue
            else: break
        layer_type = layer_name.split(' ')[1]
        year = layer_name.split(' ')[0]
        keys = r['features'][0]['properties'].keys()
        temp = {
            'id': [],
            'geometry': []
        }
        for k in keys:
            temp[k] = []
        for f in r['features']:
            temp['geometry'].append(shape(f['geometry']))
            temp['id'].append(f['id'])
            for k in keys:
                temp[k].append(f['properties'][k])
        gdf = gpd.GeoDataFrame(temp, crs='EPSG:3067')
        gdf.to_file(outpath/f'{layer_type}.gpkg', layer=year)
