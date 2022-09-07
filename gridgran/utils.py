"""Utility functions to help classes associated with gridgran package.

Main functionality is to open files, prepare dataframes to grids and then
classify cells according to number of households and population
"""

import geopandas as gpd
import numpy as np
from types import SimpleNamespace

import gridgran


def make_df(geopackage, layer, geom_type):
    """Opens and returns geopackage layer and names columns accordingly

    Parameters:
    -----------
    geopackage : (Path)
        Path to gpkg
    layer : (str)
        Layer within gpkg
    geom_type : (str)
        Point or grid

    Returns:
    --------
    df : gpd.GeoDataFrame
        Opened geodataframe with columns named appropriately according to
        geometry type
    """
    df = gpd.read_file(geopackage, layer=layer)
    if geom_type == 'grid':
        df = df[[x for x in df.columns if x.startswith(('Grid', 'geometry'))]]
        gridcol = [x for x in df.columns if x.startswith('Grid')][0]
        df.rename(columns={gridcol: f'ID{layer}'}, inplace=True)
    else:
        df = df[
            [x for x in df.columns if x.startswith(('people', 'geometry',
                                                    'uprn'))]]
        df['h'] = 1
        df.rename(columns={'people': 'p'}, inplace=True)
    try:
        assert df.crs == 'epsg:27700'
    except AssertionError:
        df = df.to_crs(27700)
    return df


def prep_df(df, geom_type):
    """Same funtionality as make_df() but takes point or grid DATAFRAMES
     rather than geopackage - returns geopackage layer and names columns
      accordingly

    Parameters:
    ------------
    df : gpd.GeoDataFrame
        Point or grid geodataframe to be prepped
    geom_type : gpd.GeoDataFrame
        Indicator as to whether gdf is point or grid polygon
    Returns:
    ---------
    df : gpd.GeoDataFrame
        Function checks/changes col names and projection
    """
    if geom_type == 'grid':
        df = df.copy()[[x for x in df.columns if x.startswith(('Grid',
                                                               'geometry'))]]
        gridcol = [x for x in df.columns if x.startswith('Grid')][0]
        df.rename(columns={gridcol: 'ID125m'}, inplace=True)
    else:
        df = df.copy()[
            [x for x in df.columns if x.startswith(('people', 'geometry',
                                                    'uprn'))]]
        df['h'] = 1
        df.rename(columns={'people': 'p'}, inplace=True)
    try:
        assert df.crs == 'epsg:27700'
    except AssertionError:
        df = df.to_crs(27700)
    return df


def make_index(row, repl_val, index_no):
    """Assign higher level (250m/500m/1000m) ids based on lowest level id

    Parameters:
    -----------
    row : pd.series
        Row from dataframe
    repl_val : str
        Characters to insert
    index_no : int
        Index number in ID str in which to insert the repl_val

    Returns:
    ---------
    new_index : str
        Higher level index
    """
    if index_no < -1:
        new_index = row.ID125m[:-3] + repl_val + row.ID125m[index_no + 1:]
    else:
        new_index = row.ID125m[:-3] + repl_val
    return new_index


def insert_index(gdf_125):
    """Apply make_index() on each row to add higher level indexes

    Parameters:
    -----------
    gdf_125 : gpd.GeoDataFrame
        Lower level dataframe to apply higher level ID's to

    Returns:
    --------
    gdf_125 : gpd.GeoDataFrame
        Input geodataframe with 3 additional higher levels of ID's applied
    """
    gdf = gdf_125.copy()
    gdf['ID250m'] = gdf_125.apply(make_index, args=['0', -3], axis=1)
    gdf['ID500m'] = gdf_125.apply(make_index, args=['00', -2], axis=1)
    gdf['ID1000m'] = gdf_125.apply(make_index, args=['000', -1], axis=1)
    return gdf


def join_pts_to_grid(gdf_grid, gdf_pts):
    """Returns gdf_grid joined to points with NA's converted to 0 in p and h \
    cols and index_right removed

    Parameters:
    -----------
    gdf_grid : gpd.GeoDataFrame
        Grid dataframe
    gdf_pts : gpd.GeoDataFrame
        Pt dataframe

    Returns
    ---------
    df_grid : pd.DataFrame
        Grid dataframe joined to points with geometries removed
    """
    gdf_grid = gpd.sjoin(gdf_grid, gdf_pts, how='left',
                         predicate='intersects')
    gdf_grid[['p', 'h']] = gdf_grid[['p', 'h']].fillna(value=0)
    gdf_grid = gdf_grid[
        [x for x in gdf_grid.columns if not x == 'index_right']]
    del gdf_grid['geometry']
    return gdf_grid


def classify_cells(row):
    """Returns classification of cells based on the class of population and \
    households

    HIERARCHY NEEDS TO BE DECIDED WHERE CLASSES ARE DIFFERENT

    Returns class if pop and houses are equal
    Else returns lowest class

    Parameters:
    -----------
    row : pd.Series
        Row of dataframe

    Returns:
    --------
    cls : int
        Class corresponding to lowest class of population OR households
    """
    if row.p_cls == row.h_cls:
        cls = row.p_cls
    elif row.p_cls < row.h_cls:
        cls = row.p_cls
    else:
        cls = row.h_cls
    return cls


def classify_pop(row, classification_dict):
    """Returns classification of population based on number as follows:
    0: Unpopulated == 0
    1: Lower Region 0 < POP < p_1
    2: Centre Region p_1 =< POP < p_2
    3. Upper Region p_2 <= POP < p_3
    4: Over disclosure limit p_3 < POP

    Parameters:
    -----------
    row : pd.Series
        Pandas row

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

    Returns:
    --------
    class : int
        Classification as defined above
    """
    p = SimpleNamespace(**classification_dict)  # unpack items as variables
    if p.p_2:
        if row.p == 0:
            cls = 0
        elif 0 < row.p <= p.p_1:
            cls = 1
        elif p.p_1 <= row.p <= p.p_2:
            cls = 2
        elif p.p_2 <= row.p <= p.p_3:
            cls = 3
        else:
            cls = 4
    else:
        if row.p == 0:
            cls = 0
        elif 0 < row.p <= p.p_1:
            cls = 1
        elif p.p_1 <= row.p <= p.p_3:
            cls = 3
        else:
            cls = 4
    return cls


def classify_households(row, classification_dict):
    """
    Classify row based on number of households

    0: Unpopulated == 0
    1: Lower Region 0 < HS < h_1
    2: Centre Region h_1 =< HS < h_2
    3. Upper Region h_2 <= HS < h_3
    4: Over disclosure limit h_3 < HS

    Parameters:
    row : (pd.Series) - pandas row

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
        NOTE p_2 and h_2 values can be Null, in which case class 2 cells
        will not be classified (i.e. these will not be aggregated up)

    Returns:
    class : (int) - Classification as defined above
    """
    h = SimpleNamespace(**classification_dict)  # unpack items as variables
    if h.h_2:
        if row.h == 0:
            cls = 0
        elif 0 < row.h <= h.h_1:
            cls = 1
        elif h.h_1 <= row.h <= h.h_2:
            cls = 2
        elif h.h_2 <= row.h <= h.h_3:
            cls = 3
        else:
            cls = 4
    else:
        if row.h == 0:
            cls = 0
        elif 0 < row.h <= h.h_1:
            cls = 1
        elif h.h_1 <= row.h <= h.h_3:
            cls = 3
        else:
            cls = 4
    return cls


def classify(df, classification_dict, cls_2_prp=0):
    """Returns dataframe with p_cls, h_cls and overall classification \
    assigned based on h and p values

    Parameters:
    -----------
    df :    (pd.DataFrame)
        Dataframe with ID column(s) and columns for p (population) and h \
        (household) sums/counts


    Returns:
    --------
    df :    (pd.DataFrame)
        DataFrame with p_cls, h_cls And classification fields appended
    """
    if (not classification_dict["p_2"] and classification_dict["h_2"]) or (
            classification_dict["p_2"] and not classification_dict["h_2"]):
        raise gridgran.ClassificationMismatchException('Both household AND '
                                                       'population '
                                                       'should be both None '
                                                       'or both not '
                                                       'None. Please check '
                                                       'your'
                                                       'classification '
                                                       'dictionary')
    df['p_cls'] = df.apply(classify_pop,
                           classification_dict=classification_dict, axis=1)
    df['h_cls'] = df.apply(classify_households,
                           classification_dict=classification_dict, axis=1)
    df['classification'] = df.apply(classify_cells, axis=1)
    if (len(df) == 4) and (2 in df.classification.unique()):
        df = df.copy()
        cls_2_df = df[df.classification == 2]
        prp_p = cls_2_df.p.sum() / df.p.sum()
        prp_h = cls_2_df.h.sum() / df.h.sum()
        if prp_p > prp_h:
            prp = prp_p
        else:
            prp = prp_h
        if prp < cls_2_prp:
            df.loc[df.classification == 2, ['p_cls', 'h_cls',
                                            'classification']] = 1
    return df


def aggregrid(df_to_aggregate, classification_dict, level='ID125m',
              template=False, cls_2_prp=0):
    """ Returns dataframe aggregated to level. Dataframe will only contain
    level's children cell columns, and not parents. If template is true,
    a column for dissolve_id will be inserted. Before returning, rows are
    classified to indicate whether they are over threshold, or should be
    dealt with in another way.

    Parameters:
    ----------
    df_to_aggregate : (pd.DataFrame)
        DataFrame to aggregate

    level : (str)
        ID column to be used to set as index in aggregate function. Should
        be in [ID125, ID250, ID500, ID1000]

    template : (bool)
        If True, dataframe will be used for final dataframe to be joined
        back to geometry will insert a 'dissolve_id' field.

    cls_2_prp : (float)
        Proportion of Population/Households in class 2 cells relative to
        total populations in neighbours within parent cell used to determine \
        whether cells will keep classification of cell (resulting in \
        aggregation), or be reclassified to class 1 and passed around. \
        (Default = 0)
    Returns:
    ---------
    df : (pd.DataFrame)
        Dataframe aggregated to defined level and classified based on pop
        and households.
    """
    df_to_aggregate = df_to_aggregate[[x for x in df_to_aggregate.columns if
                                      x not in ['uprn', 'ID125m_MOVE',
                                                'ID250m_MOVE', 'ID500m_MOVE']]]
    IDS = ['ID125m', 'ID250m', 'ID500m', 'ID1000m']
    if level == 'ID125m':
        LEVELS = IDS
    elif level == 'ID250m':
        LEVELS = IDS[1:]
    elif level == 'ID500m':
        LEVELS = IDS[2:]
    elif level == 'ID1000m':
        LEVELS = IDS[-1]
    df = df_to_aggregate.groupby(LEVELS).sum()
    df = classify(df, classification_dict, cls_2_prp=cls_2_prp)
    if template:
        df['dissolve_id'] = np.nan
    return df.reset_index()


def get_ids(df, level, class_number):
    """Returns a list of IDs in df in level that are to be moved

    Parameters:
    -----------
    df : (pd.DataFrame)
        DataFrame from which IDS will be extracted

    level : (str)
        ID column to get list of ids from. Must be in [ID125m, ID250m,
        ID500m, ID100m]

    class_number : (int)
        Class number (0-4) to get ID's for


    Returns:
    ---------
    ids : (list)
        Unique list of IDS
    """
    ids = df[level][df.classification == class_number].to_list()
    return ids


def get_list_of_rowIDS_for_list_of_IDS(df, id_list, level):
    """Returns list of dictionaries with each holding the hierarchy of IDS
    in the list of id_list (i.e. for each row corresponding to the list's
    IDs at the level assigned, a dict is made with each level's hierarchy of \
    IDS for the row

    Parameters:
    ------------
    df : (pd.DataFrame)
        Dataframe (point in this context) from which to extact dictionaries
        of IDS

    id_list : (list)
        List of IDs at level  that will be used to identify rows from which
        to extract IDs

    level : (str)
        ID column to get list of ids from. Must be in [ID125m, ID250m,
        ID500m, ID100m]

    Returns:
    --------
    id_dicts : (list)
        List of dictionaries with each holding the all the levels' IDS
        corresponding to the row identified in id_list and level
    """
    IDS_125 = list(df["ID125m"][df[level].isin(id_list)].unique())
    id_dicts = []
    for i in IDS_125:
        row = df.loc[df.ID125m == i]
        x = {}
        x['ID125m'] = i
        x['ID250m'] = row.ID250m.values[0]
        x['ID500m'] = row.ID500m.values[0]
        x['ID1000m'] = row.ID1000m.values[0]
        id_dicts.append(x)
    return id_dicts
