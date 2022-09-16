"""
Module with functions that iterate through 4 child cells of parent cell to
check grandchild cells are over disclosure limit
"""
import numpy as np

import gridgran


def check_cells_children_are_valid(df,
                                   df_grid,
                                   df_grid_pt, current_level,
                                   parent_level,
                                   child_level,
                                   classification_dict,
                                   cls_2_prp=0, ):
    """Checks and tries to bring cells over disclosure limit and returns
    grids, points and df (aggregated to current level - 4 rows), as well as
    boolean indicating whether cell's children are over disclosure limit.

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
        keys/values as example (values can be changed but keys should remain
         the same):
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
    --------
    df_grid_checked : (pd.DataFrame)
        Processed input grid with ID125m ID's assigned accordingly based on
         what level data will be disaggregated (aggregated up or kept as is).

    df_grid_pt_checked : (pd.DataFrame)
        Processed input df_grid_pt with rows removed where data is aggregated
         up.

    df_checked : (pd.DataFrame)
        df_grid_pt_checked aggregated and classified to current level (4 rows)

    child_cells_valid : (bool)
        True if children cells above disclosure limit, else False

    """
    df_grid_checked, df_grid_pt_checked = gridgran.check_cells(
        df,
        df_grid,
        df_grid_pt,
        current_level,
        parent_level,
        child_level,
        classification_dict,
        cls_2_prp=cls_2_prp)
    df_checked = gridgran.aggregrid(df_grid_pt_checked, classification_dict,
                                    level=current_level,
                                    template=False,
                                    cls_2_prp=cls_2_prp)
    if np.all(df_grid_checked.dissolve_id.isna()):
        child_cells_valid = True  # Dissolve id hasn't been set meaning all
        # cells are above disclosure limit
    else:
        child_cells_valid = False
    if len(df_checked) < 4:
        print("CURRENT LEVEL", current_level)
        print("PARENT LEVEL", parent_level)
        print("CHILD LEVEL", child_level)
        print(
            'LOOK AT gridgran.check_cells and find out why J80069539124 is  '
            'being clipped off')
        print('df_grid_checked', df_grid_checked)
        print('df_checked', df_checked)
    return df_grid_checked, df_grid_pt_checked, df_checked, child_cells_valid


def get_children_ids(df, current_level):
    """Returns list of IDS current level column in df

    Parameters:
    -----------
    df : (pd.DataFrame)
        Dataframe from which to get ids from current level

    current_level : (str)
        Column name in df from which to extract list of IDS

    Returns:
    --------
    children_ids : (list)
        List of strings of unique ids in current level columns
    """
    children_ids = list(df[current_level].unique())
    return children_ids


def subset_by_id(df_grid, df_pt_grid, current_level, child_level, subset_id,
                 classification_dict, cls_2_prp=0):
    """Returns subset of grid and points where current level id ==
    subset_id

    Parameters:
    -----------
    df_grid : (pd.DataFrame)
        Grid dataframe to subset

    df_grid_pt : (pd.DataFrame)
        Point dataframe to subset

    current_level : (str)
        Column from which to match subset

    subset_id : (str)
        ID to use to subset dataframes

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

    cls_2_prp : (float)
        Cells classified as Cls2 and their neighbours within the same
        parent will be aggregated up to parent level class 2
        pop/households are above cls_2_prp proportion relative to total
        pop/households in all neighbours within parent cell. Proportion
        should be given (between 0 and 1) and NOT percentage (i.e. 0.1 = 10%)
        DEFAULT=0

    Returns:
    --------
    df_grid_subset : (pd.DataFrame)
        df_grid subset by id

    df_grid_pt_subset : (pd.DataFrame)
        df_grid_pt subset by id

    df_subset : (pd.DataFrame)
        df_grid_pt_subset aggregated and classified to child level (If
        current_level is ID125m, df_grid_pt_subset is aggregated to that
        level as it will not be used further
    """
    df_grid_subset = df_grid[df_grid[current_level] == subset_id]
    df_grid_pt_subset = df_pt_grid[df_pt_grid[current_level] == subset_id]
    df_subset = gridgran.aggregrid(df_grid_pt_subset, classification_dict,
                                   level=child_level,
                                   template=False,
                                   cls_2_prp=cls_2_prp)
    return df_grid_subset, df_grid_pt_subset, df_subset
