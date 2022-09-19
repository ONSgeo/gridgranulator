"""Module with utility functions to:
1. Remove 125m cells from input dataframe where they DON'T intersect BFC
highwater

2. Calculate the distance travelled by points if they have moved to another
cell.

3. Check if outputs below threshold

4. Remove duplicates in instances where points fall in more than one grid - \
i.e. where points straddle a grid's border

"""
import geopandas as gpd
import numpy as np
import pandas as pd


def remove_water_cells(gdf_125m, gdf_bfc, return_water=False,
                       index_col='GridID125m'):
    """ Returns gdf_125m with cells not intersecting gdf_bfc removed

    Parameters:
    -----------
    gdf_125m : gpd.GeoDataFrame
        Geodataframe of 125m cells that have to have water cells removed


    gdf_bfc : gpd.GeoDataFrame
        GeoDataFrame of UK highwater boundaries

    return_water : bool
        If False (Default), land grids will be returned, else water mask
        will be returned

    index_col : str
        Column in gdf_125m to use for IDS - (DEFAULT GridID125m)

    Returns:
    -------
    gdf_125m_land : gpd.GeoDataFrame
        GeoDataFrame of 125m cells with 100% water cells removed
    """
    gdf_125_int = gpd.sjoin(gdf_125m, gdf_bfc,
                            how='inner',
                            predicate='intersects').dropna()
    if return_water:
        gdf_125m_clip = gdf_125m[
            ~gdf_125m[index_col].isin(gdf_125_int[index_col])]
        gdf_125m_clip = gdf_125m_clip.assign(diss_water=1)
        gdf_125m_clip = gdf_125m_clip.dissolve(by="diss_water")
        gdf_125m_clip = gdf_125m_clip[['geometry']]
    else:
        gdf_125m_clip = gdf_125m[
            gdf_125m.GridID125m.isin(gdf_125_int[index_col])]

    return gdf_125m_clip


def calc_dist(row, points, grid_125m):
    """Calculate distance travelled between points and grid centroid"""
    if (row.ID125m != row.START_POINT) & (not pd.isna(row.uprn)):
        start_point = points[points.uprn == row.uprn].geometry.item()
        end_point = grid_125m[grid_125m.GridID125m ==
                              row.ID125m].geometry.centroid.item()
        dist = start_point.distance(end_point)
    else:
        dist = 0
    return dist


def calculate_dist_point_moved(grid_pt, points, grid_125m):
    """Returns gdf_pt with column appended for distance travelled with
    distance travelled calculated for points that were moved to another
    grid cell

    Parameters:
    -----------
    grid_pt : pd.DataFrame
        Dataframe of all points grids at the end of the algorithm following
        movement

    points : gpd.GeoDataFrame
        Dataframe of geometries of points

    grid_125m : gpd.GeoDataFrame
        GeoDataFrame of all 125m grid cells

    Returns:
    --------
    gdf_pt : pd.DataFrame
        Dataframe of all points grids at the end of the algorithm following
        movement with Distance travelled calculated in dist_moved column
    """
    grid_pt['dist_moved'] = 0
    grid_pt['dist_moved'] = grid_pt.apply(calc_dist,
                                          axis=1,
                                          points=points,
                                          grid_125m=grid_125m)
    return grid_pt


def check_threshold(row, threshold_p, threshold_h):
    """Returns True if row.p and row.h are above respective thresholds else
    False

    Parameters:
    -----------
    row : pd.Series
        Row being checked

    threshold_p : int
        Disclosure threshold for population

    threshold_h : int
        Disclosure threshold for n households

    Returns:
    ---------
    above_threshold : bool
        True if row.p and row.h are above respective thresholds else
    False
    """
    if (row.p > threshold_p) & (row.h > threshold_h):
        above_threshold = True
    else:
        above_threshold = False

    return above_threshold


def check_for_below_threshold(grid_final, threshold_p, threshold_h,
                              replace_with='minimum'):
    """Returns grid_final with boolean column added (above_threshold)
    indicating whether both P and H pass test. If the P or H are below,
    the number for the below threshold value(s) will be changed according to \
     replace_with option as follows:
        'minimum' (default) -> threshold_p or threshold_h value
        'null' -> nan/null
        'star' -> column data type will be converted to str and values will
        be replaced with an asterisk (*)

    Parameters:
    -----------
    grid_final : gpd.GeoDataFrame
        Processed and dissolved grids ready to be saved to geopackage

    threshold_p : int
        Disclosure threshold for population

    threshold_h : int
        Disclosure threshold for n households

    replace_with : str
        If p or h or both are below respective thresholds, choose to replace \
         with:
         - 'minimum' (default) -> threshold_p or threshold_h value
         - 'null' -> nan/null
         - star' -> column data type will be converted to str and values will
        be replaced with an asterisk (*)

    Returns:
    --------
    grid_final : gpd.GeoDataFrame
        Input gdf with p and h replaced where below threshold and boolean (
        above_threshold) indicating whether cell passes or fails disclosure
        test
    """
    grid_final['above_threshold'] = grid_final.apply(check_threshold,
                                                     threshold_p=threshold_p,
                                                     threshold_h=threshold_h,
                                                     axis=1)
    if replace_with == 'minimum':
        grid_final.loc[grid_final.p <= threshold_p, 'p'] = threshold_p + 1
        grid_final.loc[grid_final.h <= threshold_h, 'h'] = threshold_h + 1
    elif replace_with == 'null':
        grid_final.loc[grid_final.p <= threshold_p, 'p'] = np.nan
        grid_final.loc[grid_final.h <= threshold_h, 'h'] = np.nan
    elif replace_with == 'star':
        grid_final.loc[grid_final.p <= threshold_p, 'p'] = "*"
        grid_final.loc[grid_final.h <= threshold_h, 'h'] = "*"
    return grid_final


def make_point_df_removing_grids(pt_df):
    """Returns pt_df with rows removed where uprn is null (i.e. only save
    points and remove empty grids

    Parameters:
    -----------
    pt_df : pd.DataFrame
        Dataframe of points and grids

    Returns:
    --------
    pt_df : pd.DataFrame
        DataFrame with empty grids removed from table
    """
    pt_df = pt_df[(pt_df.uprn.notna()) & (pt_df.p > 0)]
    return pt_df


def remove_duplicates(df):
    """Remove rows where uprn is duplicated where it is straddling
           borders

       Parameters:
       -----------
        df_grid : (pd.DataFrame)
            Dataframe of all points joined to all grids and aggregated to
            125m  grids but summing pop and households
        df_grid_pt : pd.DataFrame
            Raw spatially joined data between grids and points.

        Returns:
        --------
        df_grid : (pd.DataFrame)
            Dataframe of all points joined to all grids and aggregated to
            125m  grids but summing pop and households

        df_grid_pt : pd.DataFrame
            Raw spatially joined data between grids and points.

        """
    # remove nans as a df
    # remove duplicates
    # add nans back to removed duplicates
    df_nan = df[df.uprn.isna()]
    df_valid = df[~df.uprn.isna()]
    df_valid = df_valid.drop_duplicates(subset=['uprn'], keep='first')
    df_final = pd.concat([df_valid, df_nan])
    return df_final
