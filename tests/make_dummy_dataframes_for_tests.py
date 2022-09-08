"""
Module to make dummy dataframes of grids and points to test shuffle functions

Module needs to mock dataframes matching the following scenarios:

SHUFFLE = [[1, 3], [1, 4], [3, 4], [0, 1, 3], [0, 1, 4], [0, 3, 4],[1, 3,
4],  [0, 1, 3, 4]]
"""
import numpy as np
import pandas as pd
import random

import gridgran

CLASSIFICATION_DICT = {
    'p_1': 10,
    'p_2': 40,
    'p_3': 50,
    'h_1': 5,
    'h_2': 20,
    'h_3': 25,
}

THRESHOLD_DICT = {
    '0': {
        'p_lower': 0,
        'h_lower': 0,
        'p_upper': 0,
        'h_upper': 0,

    },
    '1': {
        'p_lower': 1,
        'h_lower': 1,
        'p_upper': 9,
        'h_upper': 4,
    },
    '2': {
        'p_lower': 10,
        'h_lower': 5,
        'p_upper': 39,
        'h_upper': 19
    },
    '3': {
        'p_lower': 40,
        'h_lower': 20,
        'p_upper': 49,
        'h_upper': 24
    },
    '4': {
        'p_lower': 50,
        'h_lower': 25,
        'p_upper': 100,
        'h_upper': 50
    }
}


def reset_indices(df, df_pt):
    """Reset indices of input dataframes"""
    df = df.reset_index()
    df_pt = df_pt.reset_index()
    return df, df_pt


def get_conditions(df, df_pt, id):
    """Get bool array in dataframes where == to id"""
    cond = df['ID500m'] == id
    cond_pt = df_pt['ID500m'] == id
    return cond, cond_pt


def get_array_cls_4(sum_val, length):
    """Returns array of length that sums to greater than sum_val"""
    x = np.random.randint(1, 4, size=(
        length,))  # 4 is the mean household pop in the test dataset
    while (sum(x) < sum_val):
        length += 1
        x = np.random.randint(0, 4, size=(length,))
    return x


def get_array(sum_val, length, zeros_okay=False):
    """Returns array of length that sums to sum_val"""
    x = np.random.randint(1, 4, size=(length,))  # 4 is the mean household
    # pop in the test dataset
    while sum(x) != sum_val:
        x = np.random.randint(1, 4, size=(length,))
    return x


def classify_df_as_0(df, df_pt):
    """Returns df and df_pt_cut reclassified as 0"""
    df.loc[0, ['p', 'h', 'p_cls', 'h_cls', 'classification']] = 0
    df_pt_cut = df_pt.copy()
    df_pt_cut.loc[:, ['p', 'h']] = 0
    return df, df_pt_cut


def reclassify_using_array(df,
                           df_pt,
                           array_to_replace,
                           new_classification):
    """Reclassifies dataframe to new_classification using array_to_replace"""
    df_pt_cut = df_pt.copy().iloc[0: len(array_to_replace)]
    df_pt_to_replace = df_pt[~df_pt.ID125m.isin(
        df_pt_cut.ID125m.unique())].copy()  # Need to keep all remaining
    # cells but keep them 0
    h_ones = np.ones(len(array_to_replace))
    df_pt_cut.loc[:, 'h'] = h_ones
    df_pt_cut.loc[:, 'p'] = array_to_replace
    df_pt_to_replace.loc[:, ['p', 'h']] = 0
    df_pt_cut = pd.concat([df_pt_cut, df_pt_to_replace])
    df.loc[0, ['p', 'h', 'p_cls', 'h_cls', 'classification']] = [
        df_pt_cut.p.sum(), len(array_to_replace), new_classification,
        new_classification, new_classification]
    return df, df_pt_cut, df_pt_to_replace


def replace_values_in_points_and_grids(df, df_pt, col_name, array_to_replace,
                                       new_classification):
    """Sets input df_pt to length of array_to_replace and replaces p values
    using array_to_replace"""
    df = df.reset_index(drop=True)
    if new_classification == 0:
        df, df_pt_cut = classify_df_as_0(df, df_pt)
    else:
        df, df_pt_cut, df_pt_to_replace = reclassify_using_array(
            df,
            df_pt,
            array_to_replace,
            new_classification)
    return df, df_pt_cut


def get_subset_dfs(df, df_pt, cond, cond_pt):
    """Returns df and df_pt subset to cond and cond_pt"""
    df_subset = df[cond]
    df_pt_subset = df_pt[cond_pt]
    return df_subset, df_pt_subset


def get_pop_and_households(class_level, upper_lower):
    """Returns p and h based on upper_lower in THRESHOLD_DICT"""
    if upper_lower == 'upper':
        p = 'p_upper'
        h = 'h_upper'
    elif upper_lower == 'lower':
        p = 'p_lower'
        h = 'h_lower'
    else:  # RANDOM IN RANGE OF UPPER AND LOWER
        p = random.randint(THRESHOLD_DICT[class_level]['p_lower'],
                           THRESHOLD_DICT[class_level]['p_upper'])
        h = random.randint(THRESHOLD_DICT[class_level]['h_lower'],
                           THRESHOLD_DICT[class_level]['h_upper'])
    return p, h


def make_cls_4_subset(df_subset, df_pt_subset, remake_cls_4, p, h,
                      class_level, index, cell_config):
    """Creates class 4 df_subset and df_pt_subset"""
    if remake_cls_4:
        if isinstance(p, str):
            pop_array = get_array_cls_4(THRESHOLD_DICT[class_level][p],
                                        THRESHOLD_DICT[class_level][
                                            h])  # Make a dictionary of values
        else:
            pop_array = get_array_cls_4(p, h)
        df_subset, df_pt_subset = replace_values_in_points_and_grids(
            df_subset, df_pt_subset, 'ID500m', pop_array,
            cell_config[index])
    return df_subset, df_pt_subset


def make_other_cls_subset(df_subset, df_pt_subset, p, h, class_level,
                          cell_config, index):
    """Makes non-class 4 df_subset and df_pt_subset"""
    if isinstance(p, str):
        pop_array = get_array(THRESHOLD_DICT[class_level][p],
                              THRESHOLD_DICT[class_level][
                                  h])  # Make a dictionary of values
    else:
        pop_array = get_array(p, h)
    df_subset, df_pt_subset = replace_values_in_points_and_grids(
        df_subset, df_pt_subset, 'ID500m', pop_array, cell_config[
            index])
    return df_subset, df_pt_subset


def remake_subsets(df_subset, df_pt_subset, remake_cls_4, p, h, class_level,
                   index, cell_config):
    """Returns remade df_subset, df_pt_subset to match class_level"""
    if cell_config[index] == 4:
        df_subset, df_pt_subset = make_cls_4_subset(df_subset,
                                                    df_pt_subset,
                                                    remake_cls_4, p, h,
                                                    class_level, index,
                                                    cell_config)
    else:
        df_subset, df_pt_subset = make_other_cls_subset(df_subset,
                                                        df_pt_subset, p,
                                                        h, class_level,
                                                        cell_config, index)
    return df_subset, df_pt_subset


def concat_dataframes(df_list, df_pt_list):
    """Makes dataframes from lists"""
    if len(df_list) > 0:
        df_final = pd.concat(df_list)
    if len(df_pt_list) > 0:
        df_pt_final = pd.concat(df_pt_list)
    return df_final, df_pt_final


def make_global_df(df_final, df_pt_final, df_GLOBAL):
    """Returns aggregate global grid of df_pt_final"""
    if not df_pt_final.empty:
        df_pt_agg = gridgran.aggregrid(df_pt_final, CLASSIFICATION_DICT)[
            ['ID125m', 'p', 'h',
             'p_cls', 'h_cls',
             'classification']].set_index('ID125m')
        df_grid_GLOBAL = df_GLOBAL[
            ['ID125m', 'ID250m', 'ID500m', 'ID1000m',
             'dissolve_id']].set_index(
            'ID125m').join(df_pt_agg)
    else:
        df_pt_agg = df_pt_final.copy()[['ID125m', 'p', 'h']].set_index(
            'ID125m')
        df_grid_GLOBAL = df_GLOBAL[
            ['ID125m', 'ID250m', 'ID500m', 'ID1000m',
             'dissolve_id']].set_index(
            'ID125m').join(df_pt_agg)
        df_grid_GLOBAL['p_cls'] = 0
        df_grid_GLOBAL['h_cls'] = 0
        df_grid_GLOBAL['classification'] = 0
    df_grid_GLOBAL.fillna(0, inplace=True)
    del df_grid_GLOBAL['dissolve_id']  # Move dissolve_id to end of columns
    df_grid_GLOBAL[
        'dissolve_id'] = np.nan  # Move dissolve_id to end of columns
    # df_grid_GLOBAL.dissolve_id = np.nan
    return df_grid_GLOBAL


def make_any_combination(df, df_pt, df_GLOBAL, cell_config,
                         upper_lower='upper',
                         remake_cls_4=False):
    """Returns dataframe with cells classified to combination in cell_fig,
    along with corresponding points matching class criteria"""
    df, df_pt = reset_indices(df, df_pt)
    df_list = []
    df_pt_list = []
    for index, row in df.iterrows():
        cell_id = row['ID500m']
        cond, cond_pt = get_conditions(df, df_pt, cell_id)
        df_subset, df_pt_subset = get_subset_dfs(df, df_pt, cond, cond_pt)
        class_level = str(cell_config[index])
        p, h = get_pop_and_households(class_level, upper_lower)
        df_subset, df_pt_subset = remake_subsets(df_subset, df_pt_subset,
                                                 remake_cls_4, p, h,
                                                 class_level, index,
                                                 cell_config)
        if not df_subset.empty:
            df_list.append(df_subset)
        if not df_pt_subset.empty:
            df_pt_list.append(df_pt_subset)
    df_final, df_pt_final = concat_dataframes(df_list, df_pt_list)
    df_grid_GLOBAL = make_global_df(df_final, df_pt_final, df_GLOBAL)
    del df_final["index"]
    del df_pt_final["index"]
    return df_final, df_pt_final, df_grid_GLOBAL.reset_index()
