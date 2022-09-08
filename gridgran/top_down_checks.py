"""Module to prep data for first stage of checks

1. Opens grids and preps ID's for all levels
2. SJoins to points
3. Makes another dataframe aggregated to 125m cells
4. Carries out checks on 500m level - taking relevant rows from step 2 to an \
 output dataframe
5. Sets dissolve ID to that of parent (current level if 125m) if it cannot
go any finer
"""
import numpy as np
import gridgran


def prep_points_and_grid_dataframes(gpkg, classification_dict, cls_2_prp=0):
    """Returns gdf_grid spatially joined to points in gpkg and another df \
    with grids/points aggregated to 125m.
    These datasets can be used to carry out checks in grids in different
    levels (df_grid) AND to move points around (df_grid_pt)

    Parameters:
    -----------
    gpkg : (Path)
        Path to input geopackage containing the grid data and points data

    classification_dict : (dict)
        Dictionary with keys/values for household thresholds with following
        keys/values as example (values can be changed but keys should
        remain the same):
        {
        'p_1': 10,
        'p_2': 40,
        'p_3': 50,
        'h_1': 5,
        'h_2': 20,
        'h_3': 25,
        }

    cls_2_prp : (float)
        Cells classified as Cls2 and their neighbours within the same
        parent will be aggregated up to parent level class 2
        pop/households are above cls_2_prp proportion relative to total
        pop/households in all neighbours within parent cell. Proportion
        should be given (between 0 and 1) and NOT percentage (i.e. 0.1 = 10%)
        DEFAULT=0

    Returns
    -------
    df_grid : pd.DataFrame
        Dataframe of all points joined to all grids and aggregated to 125m
        grids but summing pop and households

    df_grid_pt : pd.DataFrame
        Raw spatially joined data between grids and points.
    """
    gdf = gridgran.make_df(gpkg, '125m', 'grid')
    gdf_pt = gridgran.make_df(gpkg, 'points', 'point')
    df_grid_pt = gridgran.join_pts_to_grid(gdf, gdf_pt)
    df_grid_pt = gridgran.insert_index(df_grid_pt)
    # Keep a record of where points have moved
    df_grid_pt['ID500m_LEVEL_MOVE_ORIGIN'] = np.nan
    df_grid_pt['ID250m_LEVEL_MOVE_ORIGIN'] = np.nan
    df_grid_pt["ID125m_LEVEL_MOVE_ORIGIN"] = np.nan
    # df_grid_pt["START_POINT"] = df_grid_pt.copy()
    df_grid_pt = df_grid_pt.assign(START_POINT=df_grid_pt.ID125m.copy())
    df_grid = gridgran.aggregrid(df_grid_pt, classification_dict,
                                 level='ID125m', template=True,
                                 cls_2_prp=cls_2_prp)
    return df_grid, df_grid_pt


def prep_points_and_grid_from_dataframes(df_grids, df_points,
                                         classification_dict,
                                         cls_2_prp=0):
    """
    Returns gdf_grid spatially joined to df_points and another aggregated to \
    125m
    Parameters:
    -----------
    df_grid : (gpd.GeoDataFrame)
        125m grids corresponding spatially to extend of points
    df_points : (gpd.GeoDataFrame)
        Dataframe of points to disaggregate

    classification_dict : (dict)
            Dictionary with keys/values for household thresholds with following
            keys/values as example (values can be changed but keys should
            remain the same):
            {
            'p_1': 10,
            'p_2': 40,
            'p_3': 50,
            'h_1': 5,
            'h_2': 20,
            'h_3': 25,
            }

    cls_2_prp : (float)
        Cells classified as Cls2 and their neighbours within the same
        parent will be aggregated up to parent level class 2
        pop/households are above cls_2_prp proportion relative to total
        pop/households in all neighbours within parent cell. Proportion
        should be given (between 0 and 1) and NOT percentage (i.e. 0.1 = 10%)
        DEFAULT=0
    Returns:
    ---------
    df_grid : (pd.DataFrame)
        Dataframe of all points joined to all grids and aggregated to 125m
        grids but summing pop and households
    df_grid_pt : pd.DataFrame
        Raw spatially joined data between grids and points.
    """
    gdf = gridgran.prep_df(df_grids, 'grid')
    gdf_pt = gridgran.prep_df(df_points, 'point')
    df_grid_pt = gridgran.join_pts_to_grid(gdf, gdf_pt)
    df_grid_pt = gridgran.remove_duplicates(df_grid_pt)  # Remove duplicates
    # in cases where points touch borders
    df_grid_pt = gridgran.insert_index(df_grid_pt)
    # Keep a record of where points have moved
    df_grid_pt['ID500m_LEVEL_MOVE_ORIGIN'] = np.nan
    df_grid_pt['ID250m_LEVEL_MOVE_ORIGIN'] = np.nan
    df_grid_pt["ID125m_LEVEL_MOVE_ORIGIN"] = np.nan
    df_grid_pt = df_grid_pt.assign(START_POINT=df_grid_pt.ID125m.copy())
    df_grid = gridgran.aggregrid(df_grid_pt,
                                 classification_dict,
                                 level='ID125m',
                                 template=True,
                                 cls_2_prp=cls_2_prp)
    return df_grid, df_grid_pt


def check_cells(df,
                df_grid,
                df_grid_pt,
                current_level,
                parent_level,
                child_level,
                classification_dict,
                num_iterations=100,
                sample_increase_frequency=10,
                number_to_increase_sample=1,
                cls_2_prp=0):
    """
    Checks 4 children in each cell of the df current grid level (i.e every
    500m cell in each 1km cell) to ascertain whether grid should be
    aggregated at current level, or if it can be passed to a higher resolution

    Class Conditions:
    -----------
    1. If any cells cls 2 : Give rows' IDs all parents' IDS in df_grid and
    remove ALL 125m rows from df_grid_pt

    2. All 4 OR/AND 0 : pass back as is

    3. All 3 OR 1 : Give rows' IDs all parents' IDS in df_grid and remove
    ALL 125m rows from df_grid_pt

    4. All cells 0 AND 1 : Give rows' IDs all parents' IDS in df_grid and
    remove ALL 125m rows from df_grid_pt

    5. Cells in [0,1,3,4] : Try to move rows around to make everything 4 OR
    0  else give rows' IDs all parents' IDS in df_grid and remove ALL 125m
    rows from df_grid_pt


    Parameters:
    -----------
    df :    (pd.DataFrame)
        DataFrame aggregated to current level with classifications in assigned

    df_grid : (pd.DataFrame)
        Grid dataframe aggregated to 125m level

    df_grid_pt : (pd.DataFrame)
        Grids joined to points

    current_level : (str)
        Current resolution column in df being processed (i.e. 500m, 100m etc)

    parent_level :  (str)
        Parent level of current level being processed

    child_level :   (str)
        Child level of current level being processed

    classification_dict : (dict)
        Dictionary with keys/values for household thresholds with following
        keys/values as example (values can be changed but keys should remain \
        the same):
        {
        'p_1': 10,
        'p_2': 40,
        'p_3': 50,
        'h_1': 5,
        'h_2': 20,
        'h_3': 25,
        }

    num_iterations : (int)
        Algorithm is run in a while loop. To prevent infinite loops in
        finding optimum threshold, the while loop will break after
        num_iterations. (DEFAULT = 100)

    sample_increase_frequency : (int)
        Sample n (threshold_h) increases by number_to_increase_sample every
        sample_increase_frequency iterations in while loop. (DEFAULT = 10)

    number_to_increase_sample : (int)
        Number by which to increase the sample rows number every
        sample_increase_frequency (DEFAULT = 1)

    cls_2_prp : (float)
        Cells classified as Cls2 and their neighbours within the same
        parent will be aggregated up to parent level class 2
        pop/households are above cls_2_prp proportion relative to total
        pop/households in all neighbours within parent cell. Proportion
        should be given (between 0 and 1) and NOT percentage (i.e. 0.1 = 10%)
        DEFAULT=0

    Returns:
    --------
    df_grid : (pd.DataFrame)
        Processed input grid with ID125m ID's assigned accordingly based on
        what level data will be disaggregated (aggregated up or kept as is).
    df_grid_pt : (pd.DataFrame)
        Processed input df_grid_pt with rows removed where data is
        aggregated up.
    """
    SHUFFLE_COMBINATIONS = [[0, 1, 3, 4], [1, 3], [1, 4], [3, 4], [0, 1, 3],
                            [0, 1, 4], [0, 3, 4], [1, 3,
                                                   4]]  # Combination of
    # cells' unique classes that warrant attempt to shuffle around values
    parent_aggr = df[[parent_level, current_level, 'classification']].groupby(
        parent_level).agg('classification').unique()
    for index, i in parent_aggr.iteritems():
        if np.any(i == 2) or np.all(i == 1) or np.all(i == 3):
            df_grid = set_dissolve_id_to_parent(df_grid.copy(), parent_level,
                                                index)
        elif sorted(list(np.unique(i))) == [0, 1] or sorted(list(np.unique(
                i))) == [0, 3]:
            df_grid = set_dissolve_id_to_parent(df_grid.copy(), parent_level,
                                                index)
        elif list(np.unique(i)) in SHUFFLE_COMBINATIONS:
            df_grid, df_grid_pt = \
                shuffle_values(
                    df, df_grid, df_grid_pt,
                    current_level, parent_level,
                    child_level, index,
                    classification_dict,
                    num_iterations=num_iterations,
                    sample_increase_frequency=sample_increase_frequency,
                    number_to_increase_sample=number_to_increase_sample,
                    cls_2_prp=cls_2_prp)
    return df_grid, df_grid_pt


def set_dissolve_id_to_parent(df_grid, parent_level, index):
    """Sets dissolve ID to ID of cells' parent for use when aggregating up

    Parameters:
    -----------
    df_grid : (pd.DataFrame)
        Dataframe of all 125m grid cells/rows

    parent_level : (str)
        ID level above current level being checked

    index : (str)
        Index of parent to be aggregated up to

    Returns:
    --------
    df_grid : (pd.DataFrame)
        Grid dataframe with dissolve_id col set from null to that of parent
    """

    mask = df_grid[parent_level] == index
    df_grid.loc[mask, 'dissolve_id'] = index
    # df_grid.loc[df_grid[parent_level] == index, 'dissolve_id'] = index
    return df_grid.copy()


def shuffle_values(df,
                   df_grid,
                   df_grid_pt,
                   current_level,
                   parent_level,
                   child_level,
                   parent_index,
                   classification_dict,
                   threshold_p=50,
                   threshold_h=25,
                   num_iterations=100,
                   sample_increase_frequency=10,
                   number_to_increase_sample=1,
                   cls_2_prp=0
                   ):
    """Function attempts to move rows of df_grid_pt around into different
    ID125m values to try to get classes that allow cells to keep their
    current  resolution, otherwise they are aggregated up to parent level

    Parameters:
    ------------
    df : (pd.DataFrame)
        DataFrame aggregated to current level with classifications in assigned

    df_grid : (pd.DataFrame)
        Grid dataframe aggregated to 125m level

    df_grid_pt : (pd.DataFrame)
        Grids joined to points

    current_level : (str)
        Current resolution column in df being processed (i.e. 500m, 100m etc)

    parent_level :  (str)
        Parent level of current level being processed

    child_level :   (str)
        Child level of current level being processed

    parent_index : (str)
        Index of parent cell to be used if rows need to be aggregated up to
        parent level

     classification_dict : (dict)
        Dictionary with keys/values for household thresholds with following
        keys/values as example (values can be changed but keys should remain \
        the same):
        {
        'p_1': 10,
        'p_2': 40,
        'p_3': 50,
        'h_1': 5,
        'h_2': 20,
        'h_3': 25,
        }

    threshold_p : (int)
        Population threshold for cell to be class 4. (DEFAULT=50)

    threshold_h : (int)
        Num households to bring cell over disclosure limit

     num_iterations : (int)
        Algorithm is run in a while loop. To prevent infinite loops in
        finding optimum threshold, the while loop will break after
        num_iterations. (DEFAULT = 100)

    sample_increase_frequency : (int)
        Sample n (threshold_h) increases by number_to_increase_sample every
        sample_increase_frequency iterations in while loop. (DEFAULT = 5)

    number_to_increase_sample : (int)
        Number by which to increase the sample rows number every
        sample_increase_frequency (DEFAULT = 5)

    cls_2_prp : (float)
        Cells classified as Cls2 and their neighbours within the same
        parent will be aggregated up to parent level class 2
        pop/households are above cls_2_prp proportion relative to total
        pop/households in all neighbours within parent cell. Proportion
        should be given (between 0 and 1) and NOT percentage (i.e. 0.1 = 10%)
        DEFAULT=0

    Returns:
    --------
    df_grid : (pd.DataFrame)
        Processed input grid with ID125m ID's assigned accordingly based on
         what level data will be disaggregated (aggregated up or kept as is).

    df_grid_pt : (pd.DataFrame)
        Processed input df_grid_pt with rows removed where data is
        aggregated up.
    """
    parent_aggr = df[[parent_level, current_level, 'classification']].groupby(
        parent_level).agg('classification').unique()
    for index, i in parent_aggr.iteritems():
        unique_vals = sorted(list(np.unique(i)))
        if unique_vals in [[1, 4], [0, 1, 4]]:
            df_grid, df_grid_pt = gridgran.move_cls_1_to_4(df, df_grid,
                                                           df_grid_pt,
                                                           current_level,
                                                           parent_level,
                                                           child_level,
                                                           classification_dict,
                                                           cls_2_prp=cls_2_prp)
        elif unique_vals in [[1, 3], [0, 1, 3], [0, 1, 3, 4], [1, 3, 4]]:
            ok_to_move, df_3_pt, df_excess_pt, df_remainder_pt = \
                gridgran.check_cls_3_can_become_cls_4(
                    df,
                    df_grid_pt,
                    df_grid,
                    current_level,
                    threshold_p=threshold_p,
                    threshold_h=threshold_h,
                    num_iterations=num_iterations,
                    sample_increase_frequency=sample_increase_frequency,
                    number_to_increase_sample=number_to_increase_sample
                    )

            if not ok_to_move:  # Aggregate up to parent level
                df_grid = set_dissolve_id_to_parent(df_grid.copy(),
                                                    parent_level,
                                                    index)
            else:
                try:
                    df_grid, df_grid_pt = gridgran.make_cls_3_to_cls_4(
                        df_3_pt,
                        df_excess_pt,
                        df_remainder_pt,
                        current_level,
                        classification_dict,
                        threshold_h=threshold_h,
                        threshold_p=threshold_p,
                        num_iterations=num_iterations,
                        sample_increase_frequency=sample_increase_frequency,
                        number_to_increase_sample=number_to_increase_sample,
                        cls_2_prp=cls_2_prp
                    )
                except gridgran.DataFrameNotOverDisclosureLimitException:
                    df_grid = set_dissolve_id_to_parent(df_grid.copy(),
                                                        parent_level, index)

    return df_grid, df_grid_pt
