import geopandas as gpd
from shapely.geometry import MultiPolygon, Polygon, GeometryCollection
from shapely.ops import unary_union
from shapely import make_valid
import os
from pathlib import Path
from tqdm import tqdm

from fastcore.script import *

def union_geometries(row):
    geometries = [geom for geom in [row['geometry_ik'], row['geometry_or']] if geom is not None]
    if geometries:
        return unary_union(geometries)
    return None

@call_parse
def flatten_layers(
    aerial_image_layer_dir:Path, # Folder containing the aerial image layer files
    orthoimage_layer_dir:Path, # Folder containing the orthoimage layer files
    outpath:Path # Where to save the resulting file
):

    ik_layers = [aerial_image_layer_dir/f for f in os.listdir(aerial_image_layer_dir)]
    or_layers = [orthoimage_layer_dir/f for f in os.listdir(orthoimage_layer_dir)]

    ik_polys = []
    or_polys = []
    ik_years = []
    or_years = []

    print('Processing aerial image layers')
    for l in tqdm(ik_layers):
        ik_years.append(int(l.stem.split('_')[0]))
        gdf = gpd.read_file(l)
        # Step 1: Fix invalid geometries using make_valid
        gdf['geometry'] = gdf['geometry'].apply(make_valid)

        # Step 2: Dissolve geometries
        dissolved = unary_union(gdf['geometry'])

        # Step 3: Extract polygons from GeometryCollection
        if isinstance(dissolved, GeometryCollection):
            polygons = [geom for geom in dissolved.geoms if isinstance(geom, Polygon) or isinstance(geom, MultiPolygon)]
            dissolved = MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]
        elif not isinstance(dissolved, MultiPolygon):
            dissolved = MultiPolygon([dissolved])

        ik_polys.append(dissolved)

    print('Processing orthophoto layers')
    for l in tqdm(or_layers):
        or_years.append(int(l.stem.split('_')[0]))
        gdf = gpd.read_file(l)
        # Step 1: Fix invalid geometries using make_valid
        gdf['geometry'] = gdf['geometry'].apply(make_valid)

        # Step 2: Dissolve geometries
        dissolved = unary_union(gdf['geometry'])

        # Step 3: Extract polygons from GeometryCollection
        if isinstance(dissolved, GeometryCollection):
            polygons = [geom for geom in dissolved.geoms if isinstance(geom, Polygon) or isinstance(geom, MultiPolygon)]
            dissolved = MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]
        elif not isinstance(dissolved, MultiPolygon):
            dissolved = MultiPolygon([dissolved])
        or_polys.append(dissolved)

    flat_ik = gpd.GeoDataFrame({'year': ik_years, 'geometry': ik_polys}, crs=gdf.crs)
    flat_or = gpd.GeoDataFrame({'year': or_years, 'geometry': or_polys}, crs=gdf.crs)

    flat = flat_ik.merge(flat_or, on='year', how='outer', suffixes=('_ik', '_or'))
    flat['geometry'] = flat.apply(union_geometries, axis=1)
    flat = flat[['year', 'geometry']]
    flat = gpd.GeoDataFrame(flat, geometry='geometry', crs=gdf.crs)
    flat.to_file(outpath)

