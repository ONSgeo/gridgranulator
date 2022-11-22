"""

Module to run when preparing data geopackage to processing.

make_geopackage() function will call helper functions in this script to
prepare a geopackage holding all the points grids and oa layers used
processing small areas in the grid_granulator class


**NOTE - This should not be used for national level extents. Only one or a
few neighbouring Local authorities

** NOTE 2 - This will only extract grids to the extent of points' total
bounds, and NOT to the bounds of the LA that is input

"""
import geopandas as gpd


def get_la_geoms(la_path, la_ids, la_col, layer=None):
    """
    Returns geometries of local authorities corresponding to la_ids held in
    shapefile/geopackage at la_path. Layer should be used in the case of
    layers within la_path geopackages, otherwise shapefile

    Parameters:
    -----------
    la_path : str/Path
        Path to LA geopackage or shapfile

    la_ids : list
        List of LAs to extract

    la_col : str
        Column of la_ids in la_path shp/geopackage

    layer : None/str
        Layer in geopackage if any - Default None

    Returns:
    --------
    la_gdf : gpd.GeoDataframe
        Geodataframe of LA(s) to extract.
    """
    if not layer:
        gdf = gpd.read_file(la_path)
    else:
        gdf = gpd.read_file(la_path, layer=layer)
    gdf = gdf[gdf[la_col].isin(la_ids)]
    gdf = gdf[[la_col, 'geometry']]
    return gdf


def get_points(gdf, pt_path, layer=None):
    """Get points within LA - This will use intersect so be careful not to
    duplicate points on neigbouring LAs that touch borders"""
    bbox = list(gdf.total_bounds)
    if not layer:
        pts_bbox = gpd.read_file(pt_path, bbox=bbox)
    else:
        pts_bbox = gpd.read_file(pt_path, layer=layer, bbox=bbox)
    gdf['diss'] = 1
    gdf_diss = gdf.dissolve(by='diss')
    pts = pts_bbox.sjoin(gdf_diss, how='left', predicate='intersects').dropna()
    pts = pts[[x for x in pts_bbox.columns]]
    return pts


def get_grids(pts, path_1km, path_125m, layer_1km=None, layer_125m=None):
    """Extracts grids from path_1km (layer_1km) and path_125 (layer_125) to
    extent of bounds of pts

    Parameters:
    pts : gpd.GeoDataFrame
        Points used to set extent of extraction

    path_1km : Path/str
        Path to 1km grid

    path_125m : Path/str
        Path to 125m grid

    layer_1km : None/str
        Layer for 1km if in gpkg (else shapefile or only layer in gpkg)

    layer_125 : None/str
        Layer for 125m if in gpkg (else shapefile or only layer in gpkg)

    Returns:
    --------
    grid_1km : gpd.GeoDataFrame
        1km grid

    grid_125m : gpd.GeoDataFrame
        125m grid
    """
    grid_1km = gpd.read_file(path_1km, layer=layer_1km, mask=pts)
    grid_125m = gpd.read_file(path_125m, layer=layer_125m, mask=grid_1km)
    grid_125m['ID1km'] = grid_125m['GridID125m'].str[:-3] + '000'
    grid_125m = grid_125m[grid_125m.ID1km.isin(
        grid_1km.GridID1km.unique())]
    return grid_1km, grid_125m


def make_points_geopackage(la_path,
                           la_ids,
                           la_col,
                           pt_path,
                           out_path,
                           path_1km,
                           path_125m,
                           out_layer=None,
                           uprn_col=None,
                           la_layer=None,
                           pt_layer=None,
                           pt_pop_col='people',
                           layer_1km=None,
                           layer_125m=None):
    """Function to call all utility helpers to prepare data and save to
    geopackage ready for processing

    Parameters:
    -----------
    la_path : Path/str
        Path to local authorities

    la_ids : list
        List of ids to extract from local authorities

    la_col : str
        ID column in local authorities

    pt_path : Path/str
        Path to points

    out_path : Path/str
        Path to output geopackage

    path_1km : Path/str
        Path to 1km grid

    path_125m : Path/str
        Path to 125m grid

    uprn_col : None/str
        UPRN column in points. If none, index will be used to generate uprn

    la_layer : None/str
        LA layer in la_path if geopackage, None used to shapefile

    lpt_layer : None/str
        Point layer in pt_path if geopackage, None used to shapefile

    layer_1km : None/str
        Layer for 1km if in gpkg (else shapefile or only layer in gpkg)

    layer_125 : None/str
        Layer for 125m if in gpkg (else shapefile or only layer in gpkg)
    """
    gdf = get_la_geoms(la_path, la_ids, la_col, layer=la_layer)
    pts = get_points(gdf, pt_path, layer=pt_layer)
    if not uprn_col:
        if 'uprn' not in pts.columns:
            pts['uprn'] = pts.index.copy()
    else:
        pts.rename(columns={uprn_col: 'uprn'}, inplace=True)
    if pt_pop_col in pts.columns:
        if pt_pop_col != 'people':
            pts.rename(columns={pt_pop_col: 'people'}, inplace=True)
    else:
        raise Exception(f'{pt_pop_col} is not a column in point feature. '
                        f'Please set the population column in the '
                        f'"pt_pop_col" key word argument')
    if not out_layer:
        pts.to_file(out_path, index=False)
    else:
        pts.to_file(out_path, layer=out_layer, index=False, driver='GPKG')
    grid_1km, grid_125m = get_grids(
        pts,
        path_1km,
        path_125m,
        layer_1km=layer_1km,
        layer_125m=layer_125m
    )
    grid_1km.to_file(out_path, layer='1000m', index=False, driver='GPKG')
    grid_125m.to_file(out_path, layer='125m', index=False, driver='GPKG')
