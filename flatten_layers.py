import geopandas as gpd
from shapely.geometry import MultiPolygon, Polygon, GeometryCollection
from shapely.ops import unary_union
from shapely import make_valid
import fiona

def union_geometries(row):
    geometries = [geom for geom in [row['geometry_ik'], row['geometry_or']] if geom is not None]
    if geometries:
        return unary_union(geometries)
    return None

ik_layers = fiona.listlayers('data/ilmakuvat.gpkg')
or_layers = fiona.listlayers('data/ortot.gpkg')

ik_polys = []
or_polys = []
ik_years = []
or_years = []

for l in ik_layers:
    ik_years.append(int(l))
    gdf = gpd.read_file('data/ilmakuvat.gpkg', layer=l)
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

for l in or_layers:
    or_years.append(int(l))
    gdf = gpd.read_file('data/ortot.gpkg', layer=l)
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
flat.to_file('data/index_layers.geojson')