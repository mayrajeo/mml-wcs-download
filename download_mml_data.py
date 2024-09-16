import json
import owslib.wcs as wcs
import geopandas as gpd
from pathlib import Path
import numpy as np
import base64
import io
import os
import shapely
import rasterio.merge as riomerge
from itertools import product
from PIL import Image

#from multiprocessing.pool import Pool
from joblib import Parallel, delayed

from fastcore.script import *

def read_api_key() -> str:
    "Read api key from `secret/creds.json`"
    with open('secret/creds.json') as f: keys = json.load(f)
    return keys['mml_api_key']

def check_years(year_layers:gpd.GeoDataFrame, location:shapely.geometry):
    "Check which `year_layers` intersect with `location`"
    return sorted(year_layers[year_layers.geometry.intersects(location)].year.unique())

def get_wcs_img(bounds:list[float,float,float,float], outfile:Path, year:int=2023,
                max_retries:int=5, false_color:bool=False) -> None:
    """
    Download data from NLS Finland WCS, provided it exists. If the image exists, 
    it is saved to `outfile`. If not, nothing is saved. Whether the image is a valid aerial
    image is checked by comparing the maximum and minimum values of the image. If they are both 
    identical (e.g. 0), then no data is saved.
    
    For years before 2009, the data is downloaded from `ortokuva_mustavalko` (black-and-white images), 
    otherwise either `ortokuva_vari` or `ortokuva_vaaravari` (if `false_color` == True) is used.

    Args:
        bounds (list[float,float,float,float]): Bounding box ([`xmin`, `ymin`, `xmax`, `ymax`]) in EPSG:3067 coordinates
                                                for the area to download. Maximum size 2kmx2km.
        outfile (Path): Path to save the outfile
        year (int): Which year to download
        max_retries (int): How many times to retry the download. Default 5
        false_color (bool): Whether to download RGB or NIR-R-G image. Only valid for `year` >= 2009. 
    """

    mml_wcs_url = 'https://avoin-karttakuva.maanmittauslaitos.fi/ortokuvat-ja-korkeusmallit/wcs/v2'
    mml_wcs = None
    api_key = read_api_key()
    for n in range(max_retries+1):
        if int(year) < 2009: layer = 'ortokuva_mustavalko'
        else: layer = 'ortokuva_vari'
        if false_color == True: layer = 'ortokuva_vaaravari'
        if layer == 'ortokuva_vaaravari' and int(year) < 2009: 
            print('False color images available only for year 2009 onward')
            return
        try:
        # Try to request an image
            if mml_wcs is None:
                mml_wcs = wcs.WebCoverageService(
                    mml_wcs_url, headers={
                    'Authorization': f'Basic {str(base64.b64encode(api_key.encode()))[2:-1]}Og=='
                    })
            img_rgb = mml_wcs.getCoverage(identifier=[layer],
                                          crs='EPSG:3067',
                                          subsets=[('E', bounds[0], bounds[2]), 
                                                    ('N', bounds[1], bounds[3]), 
                                                    ('time', f'{year}-12-31T00:00:00.000Z')],
                                          format='image/tiff')
        except Exception as e:
            # Connectionerror or something like that, try again
            if n < max_retries:
                print(f'Failed to access, retry {n}')
            else: 
                print('Failed to access, skipping')
                return
        else:
            if layer == 'ortokuva_mustavalko':
                vmin, vmax = Image.open(io.BytesIO(img_rgb.read())).getextrema()
            else: 
                r, g, b = Image.open(io.BytesIO(img_rgb.read())).getextrema()
                vmin = min(r[0], g[0], b[0])
                vmax = max(r[1], g[1], b[1])
            if vmin == vmax: 
                return # Empty data, no need to save
            else: 
                break

    with open(outfile, 'wb') as out:
        print(f'Saving file {outfile}')
        out.write(img_rgb.read())
    return None

def process_point_data(patch_id, geom, year_layer_path, imsize, outpath, false_color):
    """
    Download all available historical aerial images as `imsize` times `imsize` pixel images 
    centered around `geom` and save them to `outpath`
    """
    if year_layer_path: 
        year_layers = gpd.read_file(year_layer_path)
        years = check_years(year_layers, geom)
    else:
        years = range(1931, 2024)
    bounds = geom.buffer(imsize//4, cap_style='square', join_style='mitre').bounds
    min_x = np.floor(bounds[0])
    min_y = np.floor(bounds[1])
    max_x = min_x + imsize//2
    max_y = min_y + imsize//2
    if not os.path.exists(outpath/f'{patch_id}'):
        os.makedirs(outpath/f'{patch_id}')
    for y in years:
        if os.path.exists(outpath/f'{patch_id}/{y}.tif'): continue
        get_wcs_img(bounds=[float(min_x), float(min_y), float(max_x), float(max_y)],
                    outfile=outpath/f'{patch_id}/{y}.tif',
                    year=y,
                    false_color=false_color)
    print(f'Finished with {patch_id}')
    return

def process_polygon_data(patch_id, geom, year_layer_path, outpath, false_color):
    """
    Download all available historical aerial image data that can be found within the bounding box of `geom`
    and save them to `outpath`
    """
    if year_layer_path: 
        year_layers = gpd.read_file(year_layer_path)
        years = check_years(year_layers, geom)
    else:
        years = range(1931, 2024)
    min_x, min_y, max_x, max_y = geom.bounds
    # Get width and height
    w = int(np.ceil(max_x-min_x))
    h = int(np.ceil(max_y-min_y))
    if not os.path.exists(outpath/f'{patch_id}'):
        os.makedirs(outpath/f'{patch_id}')
    for y in years:
        if os.path.exists(outpath/f'{patch_id}/{y}.tif'): continue
        for i, (dx, dy) in enumerate(product(range(0, w, 2000), range(0, h, 2000))):
            get_wcs_img(bounds=[float(min_x+dx), 
                                float(min_y+dy), 
                                float(min(max_x, min_x+dx+2000)), 
                                float(min(max_y, min_y+dy+2000))],
                        outfile=outpath/f'{patch_id}/temp_{y}_{i}.tif',
                        year=y,
                        false_color=false_color)
        files_to_merge = [outpath/f'{patch_id}/{f}' for f in os.listdir(outpath/f'{patch_id}') if 'temp' in f]
        if len(files_to_merge) == 0: continue
        riomerge.merge(files_to_merge, dst_path=outpath/f'{patch_id}/{y}.tif',
                       dst_kwds={'compress':'lzw', 'predictor':2, 'BIGTIFF':'YES'})
        for f in files_to_merge: os.remove(f)
    print(f'Finished with {patch_id}')
    return


@call_parse
def download_mml_data(
    locations:Path, # Path to GIS data containing locations either as points or polygons
    outpath:Path, # Directory to save the results
    year_layer_path:Path=None, # Data of historical aerial campaigns used to filter possible years to process
    id_column:str=None, # Which column is used to identify the locations. If None, index is used.
    false_color:bool=False, # Whether to download NIR-R-G images. Available only for 2009 and later
    imsize:int=256 # If locations are point data, the size of the images to download
):
    """
    Download all available aerial images from `locations` and save them to `outpath`. 
    If `locations` are Point data, all points are buffered so that `imsize` times `imsize` images centered around
    the points are produced. 
    If `locations` are Polygon data, resulting images cover the orthogonal bounding boxes of the locations.
    If `year_layer_path` is provided, possible years are filtered so that only years where there is a possibility
    of aerial campaing are processed. Otherwise, all years from 1931 to 2024 are processed.
    """

    if not os.path.exists(outpath): os.makedirs(outpath)

    gdf = gpd.read_file(locations).to_crs('EPSG:3067')

    # Infer geometry type

    geom_type = gdf.geom_type.unique()

    # Check for unsupported geometry types
    if 'LineString' in geom_type:
        print('LineString geometries are not supported')
        return
    if 'LinearRing' in geom_type:
        print('LinearRing geometries are not supported')
        return
    if 'MultiLineString' in geom_type:
        print('MultiLineString geometries are not supported')
        return
    if 'GeometryCollection' in geom_type:
        print('GeometryCollection geometries are not supported')
        return
    if 'MultiPoint' in geom_type:
        print('MultiPoint geometries are not supported')
        return
    if 'MultiPolygon' in geom_type:
        print('MultiPolygon geometries are not supported')
        return

    # Check for mixed types
    if len(geom_type) > 1:
        print('Multiple geometry types detected')
        return

    # Check imsize for point data
    if imsize > 4000: 
        print(f'Largest supported image size is 2000, but {imsize} was provided.')
        return

    match geom_type[0]:
        case 'Point':
            inps = ((patch_id, geom, year_layer_path, imsize, outpath, false_color) for patch_id, geom 
                    in zip(gdf[id_column] if id_column in gdf.columns else gdf.index, gdf.geometry))
            Parallel(n_jobs=-1, backend='loky')(delayed(process_point_data)(*inp) for inp in inps)
        case 'Polygon':
            inps = ((patch_id, geom, year_layer_path, outpath, false_color) for patch_id, geom 
                    in zip(gdf[id_column] if id_column in gdf.columns else gdf.index, gdf.geometry))
            Parallel(n_jobs=-1, backend='loky')(delayed(process_polygon_data)(*inp) for inp in inps)
