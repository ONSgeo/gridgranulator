"""Unit tests for utils.py
"""

from pathlib import Path
import pytest

import geopandas as gpd
from shapely.geometry import Point, MultiPolygon

import gridgran
import tests

BASE = Path(__file__).resolve().parent.joinpath('data')
gpkg = BASE.joinpath('GRID_1km_SUBSET.gpkg')  # Test data


CLASSIFICATION_DICT = {
    'p_1': 10,
    'p_2': 40,
    'p_3': 49,
    'h_1': 5,
    'h_2': 20,
    'h_3': 24,
}

CLASSIFICATION_DICT_NO_CLS_2 = {
    'p_1': 10,
    'p_2': None,
    'p_3': 49,
    'h_1': 5,
    'h_2': None,
    'h_3': 24,
}


@pytest.fixture
def gdf():
    """Fixture to make 125m grids"""
    gdf = gridgran.make_df(gpkg, '125m', 'grid')
    yield gdf


@pytest.fixture()
def gdf_pt():
    """Fixture to make points"""
    gdf = gridgran.make_df(gpkg, 'points', 'point')
    yield gdf


@pytest.fixture()
def gdf_125_pt(gdf, gdf_pt):
    """Fixture to make grids joined to points"""
    x = gridgran.join_pts_to_grid(gdf, gdf_pt)
    yield x


@pytest.fixture()
def dfs():
    """Makes grid joined to points by not agrregated - i.e. RAW"""
    df_grid, df_grid_pt = gridgran.prep_points_and_grid_dataframes(
                                                    gpkg,
                                                    CLASSIFICATION_DICT)
    df = gridgran.aggregrid(df_grid_pt, CLASSIFICATION_DICT, level='ID500m')
    yield (df_grid, df_grid_pt, df)


def test_poly_make_df(gdf):
    """Test return of make_df polygon"""
    poly = gdf.geometry.iloc[0]
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert isinstance(poly, MultiPolygon)


def test_point_make_df(gdf_pt):
    """Test return of make_df point"""
    point = gdf_pt.geometry.iloc[0]
    assert 'uprn' in gdf_pt.columns
    assert isinstance(gdf_pt, gpd.GeoDataFrame)
    assert isinstance(point, Point)


def test_make_index(gdf):
    """Test make_index"""
    row = gdf.loc[0]
    id = gridgran.make_index(row, '0', -3)
    assert id == 'J80070856011'


def test_insert_index(gdf):
    """Test inset_index"""
    gdf_ = gdf.iloc[0:5]
    gdf_ = gridgran.insert_index(gdf_)
    assert len(gdf_) == 5
    assert 'ID1000m' in gdf_.columns
    assert 'ID500m' in gdf_.columns
    assert 'ID250m' in gdf_.columns
    assert gdf_.loc[0, 'ID500m'] == 'J80070856001'


def test_join_pts_to_grid(gdf, gdf_125_pt):
    """Test join_points_to_grid output"""
    assert len(gdf_125_pt) > len(gdf)
    assert 'geometry' not in gdf_125_pt.columns


def test_classify_pop(gdf_125_pt):
    """Test classification of population"""
    df = gdf_125_pt.groupby('ID125m').sum()
    row_1 = df.iloc[0]
    cls_1 = gridgran.classify_pop(row_1, CLASSIFICATION_DICT)
    assert cls_1 == 0
    row_2 = df.iloc[3]
    cls_2 = gridgran.classify_pop(row_2, CLASSIFICATION_DICT)
    assert cls_2 == 4


@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 2]),
    ([0, 2, 1, 4]),
    ([2, 2, 2, 2]),
    ([1, 2, 3, 4]),
    ([0, 0, 2, 2]),
    ([1, 1, 1, 4]),
    ([0, 0, 1, 4]),
    ([1, 0, 4, 4]),
    ([0, 0, 1, 4]),
    ([4, 4, 4, 1]),
])
def test_classify_pop_cls2_NONE(dfs, cell_configs):
    """Test classification of population where class 2 is None - Currently
    not used"""
    DF_GRID, DF_GRID_PT, DF = dfs
    df, df_pt, df_grid = tests.make_any_combination(DF, DF_GRID_PT,
                                                    DF_GRID, cell_configs)
    dfs = []
    for index, row in df.iterrows():
        cls_not_2 = gridgran.classify_pop(row, CLASSIFICATION_DICT_NO_CLS_2)
        df.loc[index, 'classification'] = cls_not_2
        df.loc[index, 'p_cls'] = cls_not_2
    assert 2 not in df.p_cls.unique()
    assert 2 not in df.classification.unique()


def test_classify_households(gdf_125_pt):
    """Test classify_households"""
    df = gdf_125_pt.groupby('ID125m').sum()
    row_1 = df.iloc[0]
    cls_1 = gridgran.classify_households(row_1, CLASSIFICATION_DICT)
    assert cls_1 == 0
    row_2 = df.iloc[3]
    cls_2 = gridgran.classify_households(row_2, CLASSIFICATION_DICT)
    assert cls_2 == 4


@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 2]),
    ([0, 2, 1, 4]),
    ([2, 2, 2, 2]),
    ([1, 2, 3, 4]),
    ([0, 0, 2, 2]),
    ([1, 1, 1, 4]),
    ([0, 0, 1, 4]),
    ([1, 0, 4, 4]),
    ([0, 0, 1, 4]),
    ([4, 4, 4, 1]),
])
def test_classify_households_cls2_NONE(dfs, cell_configs):
    """Test classify households where class 2 is None"""
    DF_GRID, DF_GRID_PT, DF = dfs
    df, df_pt, df_grid = tests.make_any_combination(DF, DF_GRID_PT,
                                                    DF_GRID, cell_configs)
    for index, row in df.iterrows():
        cls_not_2 = gridgran.classify_pop(row, CLASSIFICATION_DICT_NO_CLS_2)
        df.loc[index, 'classification'] = cls_not_2
        df.loc[index, 'h_cls'] = cls_not_2
    assert 2 not in df.h_cls.unique()
    assert 2 not in df.classification.unique()


def test_classify_cells(gdf_125_pt):
    """Test classify cells"""
    df = gdf_125_pt.groupby('ID125m').sum()
    df['p_cls'] = df.apply(gridgran.classify_pop,
                           classification_dict=CLASSIFICATION_DICT, axis=1)
    df['h_cls'] = df.apply(gridgran.classify_households,
                           classification_dict=CLASSIFICATION_DICT, axis=1)
    row_1 = df.iloc[0]
    cls_1 = gridgran.classify_cells(row_1)
    assert cls_1 == 0
    row_2 = df.iloc[1]
    cls_2 = gridgran.classify_cells(row_2)
    assert cls_2 == 2


@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 2]),
    ([0, 2, 1, 4]),
    ([2, 2, 2, 2]),
    ([1, 2, 3, 4]),
    ([0, 0, 2, 2]),
    ([1, 1, 1, 4]),
    ([0, 0, 1, 4]),
    ([1, 0, 4, 4]),
    ([0, 0, 1, 4]),
    ([4, 4, 4, 1]),
])
def test_classify_cells_cls2_NONE(dfs, cell_configs):
    """Test classify cells where class 2 is None"""
    DF_GRID, DF_GRID_PT, DF = dfs
    df, df_pt, df_grid = tests.make_any_combination(DF, DF_GRID_PT, DF_GRID,
                                                    cell_configs)
    for index, row in df.iterrows():
        row.p_cls = gridgran.classify_pop(row, CLASSIFICATION_DICT_NO_CLS_2)
        row.h_cls = gridgran.classify_households(row,
                                                 CLASSIFICATION_DICT_NO_CLS_2)
        cls_not_2 = gridgran.classify_cells(row)
        df.loc[index, 'classification'] = cls_not_2
    assert 2 not in df.classification.unique()


def test_classify_raises_exception_if_h_none_and_p_not_none(dfs):
    """Test exception if h or p is not none but the other is"""
    DF_GRID, DF_GRID_PT, DF = dfs
    classification_dict = CLASSIFICATION_DICT_NO_CLS_2.copy()
    classification_dict.update(p_2=5)
    with pytest.raises(gridgran.ClassificationMismatchException) as excep:
        _ = gridgran.classify(DF, classification_dict)
        print(dir(excep))


def test_classify(gdf_125_pt, gdf):
    """Test classify()"""
    df = gridgran.insert_index(gdf_125_pt)
    df = gridgran.classify(df, CLASSIFICATION_DICT)
    assert 'p_cls' in df.columns
    assert 'classification' in df.columns


@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 2]),
    ([0, 2, 1, 4]),
    ([2, 2, 2, 2]),
    ([1, 2, 3, 4]),
    ([0, 0, 2, 2]),
    ([1, 2, 2, 1]),
    ([0, 2, 1, 4]),
    ([1, 0, 2, 4]),
    ([0, 2, 1, 4]),
    ([2, 4, 4, 1]),
])
def test_classify_using_prp_of_cls2(dfs, cell_configs):
    """Test classify using proportion of class 2 cells relative to
    neighbours"""
    DF_GRID, DF_GRID_PT, DF = dfs
    df, df_pt, df_grid = tests.make_any_combination(DF, DF_GRID_PT, DF_GRID,
                                                    cell_configs)
    df = gridgran.classify(df, CLASSIFICATION_DICT)
    df_cls_2_prp = df.loc[df.classification == 2, 'p'].sum() / df.p.sum()
    df_cls = gridgran.classify(df, CLASSIFICATION_DICT, cls_2_prp=0.1)
    if df_cls_2_prp < 0.1:
        assert 2 not in df_cls.classification.unique()


@pytest.mark.parametrize("level, length", [
    ('ID125m', 64),
    ('ID250m', 16),
    ('ID500m', 4),
    ('ID1000m', 1),
])
def test_aggregrid(gdf_125_pt, gdf, level, length):
    """Test aggregrid()"""
    df_pt = gridgran.insert_index(gdf_125_pt)
    df_aggr = gridgran.aggregrid(df_pt, CLASSIFICATION_DICT, level=level,
                                 template=False)
    assert len(df_aggr) == length
    assert level in df_aggr.columns
    assert 'classification' in df_aggr.columns
    assert 'dissolve_id' not in df_aggr.columns


def test_aggregrid_as_template(gdf_125_pt, gdf):
    """Test aggregrid with template as True"""
    df_pt = gridgran.insert_index(gdf_125_pt)
    df_aggr = gridgran.aggregrid(df_pt, CLASSIFICATION_DICT, level='ID125m',
                                 template=True)
    assert 'dissolve_id' in df_aggr.columns


@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 4]),
    ([0, 0, 1, 4]),
    ([1, 0, 4, 4]),
    ([0, 0, 1, 4]),
    ([4, 4, 4, 1]),
])
def test_get_ids(dfs, cell_configs):
    """Test get_ids()"""
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid, cell_configs)

    ids1 = gridgran.get_ids(DF, 'ID500m', 1)
    ids4 = gridgran.get_ids(DF, 'ID500m', 4)
    IDS_EXPECTED1 = DF.ID500m[DF.classification == 1].to_list()
    IDS_EXPECTED4 = DF.ID500m[DF.classification == 4].to_list()
    assert ids1 == IDS_EXPECTED1
    assert ids4 == IDS_EXPECTED4


@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 4]),
    ([0, 0, 1, 4]),
    ([1, 0, 4, 4]),
    ([0, 0, 1, 4]),
    ([4, 4, 4, 1]),
    ([0, 0, 0, 0])
])
def test_get_list_of_rowsIDS_for_list_of_IDS(dfs, cell_configs):
    """Test get_list_of_rowIDS_for_list_of_IDS()"""
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid, cell_configs)
    ids1 = gridgran.get_ids(DF, 'ID500m', 1)
    id_dicts1 = gridgran.get_list_of_rowIDS_for_list_of_IDS(df_grid, ids1,
                                                            'ID500m')
    ids4 = gridgran.get_ids(DF, 'ID500m', 4)
    id_dicts4 = gridgran.get_list_of_rowIDS_for_list_of_IDS(df_grid, ids4,
                                                            'ID500m')

    assert len(id_dicts1) == (len(ids1) * 16)
    assert len(id_dicts4) == (len(ids4) * 16)
