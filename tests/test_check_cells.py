from pathlib import Path
import pytest
from pytest_mock import mocker
from unittest import mock

import geopandas as gpd
import numpy as np
import pandas as pd



import gridgran

BASE = Path(__file__).resolve().parent.joinpath('data')
gpkg = BASE.joinpath('GRID_1km_SUBSET.gpkg')

CLASSIFICATION_DICT = {
        'p_1': 10,
        'p_2': 40,
        'p_3': 50,
        'h_1': 5,
        'h_2': 20,
        'h_3': 25,
        }

@pytest.fixture
def dfs():
    """Makes grid joined to points but not agrregated - i.e. RAW"""
    df_grid, df_grid_pt = gridgran.prep_points_and_grid_dataframes(gpkg, CLASSIFICATION_DICT)
    yield (df_grid, df_grid_pt)


def test_prep_points_and_grid_dataframes(dfs):
    df_grid = dfs[0]
    df_grid_pt = dfs[1]
    assert isinstance(df_grid, pd.DataFrame)
    assert isinstance(df_grid_pt, pd.DataFrame)
    assert len(df_grid) < len(df_grid_pt)
    assert len(df_grid) == 64
    assert 'ID500m_LEVEL_MOVE_ORIGIN' in df_grid_pt.columns
    assert 'START_POINT' in df_grid_pt.columns
    assert np.all(df_grid_pt.START_POINT == df_grid_pt.ID125m)
    assert 'uprn' in df_grid_pt.columns

def test_set_dissolve_id_to_parent(dfs):
    df_grid = dfs[0]
    df_grid_pt = dfs[1]
    df_grid = gridgran.set_dissolve_id_to_parent(df_grid, "ID1000m", "J80070856000")
    assert np.all(df_grid.dissolve_id == "J80070856000")


@pytest.mark.parametrize("values, expected_id", [
    ([2, 2, 2, 2], 'J80070856000'),
    ([1, 2, 3, 4], 'J80070856000'),
    ([1, 2, 1, 2], 'J80070856000'),
    ([0, 0, 0, 2], 'J80070856000'),
])
def test_check_cells_cls_2(dfs, values, expected_id):
    """Any cells are class 2 -> Aggregate up (assign parent ID to dissolve
    ID and remove from pt dataframe"""
    df_grid, df_grid_pt = dfs
    class_df = gridgran.aggregrid(df_grid_pt, CLASSIFICATION_DICT,
                                  level='ID500m')
    for index, i in enumerate(values):
        class_df.classification.at[index] = i
    df_grid, df_grid_pt = gridgran.check_cells(class_df, df_grid, df_grid_pt,
                                               'ID500m', 'ID1000m',
                                               'ID250m', CLASSIFICATION_DICT)
    assert np.all(df_grid.dissolve_id == expected_id)


@pytest.mark.parametrize("values, expected_id", [
    ([4, 0, 4, 0],
     ['J80070856001', 'J80070856002', 'J80070856003', 'J80070856004']),
    ([4, 4, 0, 0],
     ['J80070856001', 'J80070856002', 'J80070856003', 'J80070856004']),
    ([4, 4, 4, 0],
     ['J80070856001', 'J80070856002', 'J80070856003', 'J80070856004']),
    ([0, 0, 0, 4],
     ['J80070856001', 'J80070856002', 'J80070856003', 'J80070856004']),
    ([4, 4, 4, 4],
     ['J80070856001', 'J80070856002', 'J80070856003', 'J80070856004']),
    ([0, 0, 0, 0],
     ['J80070856001', 'J80070856002', 'J80070856003', 'J80070856004'])
])
def test_check_cells_cls_4_0(dfs, values, expected_id):
    """All children are class 4 AND/OR 0 -> Leave as is (no dissolve ID and
    do not remove from points dataframe) -> This case passes down to lower
    level"""
    df_grid, df_grid_pt = dfs
    class_df = gridgran.aggregrid(df_grid_pt, CLASSIFICATION_DICT,
                                  level='ID500m')
    for index, i in enumerate(values):
        class_df.classification.at[index] = i
    df_grid, df_grid_pt = gridgran.check_cells(class_df, df_grid, df_grid_pt,
                                               'ID500m', 'ID1000m',
                                               'ID250m', CLASSIFICATION_DICT)
    assert np.all(np.isnan(df_grid.dissolve_id))
    assert expected_id == sorted(list(df_grid_pt.ID500m.unique()))


@pytest.mark.parametrize("values, expected_id", [
    ([3, 3, 3, 3], 'J80070856000'),
    ([1, 1, 1, 1], 'J80070856000'),
])
def test_check_level_cls_ALL_1_OR_3(dfs, values, expected_id):
    """All cells are class 3 or 1 -> Aggregate up (assign parent ID to dissolve
    ID and remove from pt dataframe"""
    df_grid, df_grid_pt = dfs
    class_df = gridgran.aggregrid(df_grid_pt, CLASSIFICATION_DICT,
                                  level='ID500m')
    for index, i in enumerate(values):
        class_df.classification.at[index] = i
    df_grid, df_grid_pt = gridgran.check_cells(class_df, df_grid, df_grid_pt,
                                               'ID500m', 'ID1000m',
                                               'ID250m', CLASSIFICATION_DICT)
    assert np.all(df_grid.dissolve_id == expected_id)


@pytest.mark.parametrize("values, expected_id", [
    ([0, 0, 1, 1], 'J80070856000'),
    ([0, 0, 0, 1], 'J80070856000'),
    ([0, 0, 0, 3], 'J80070856000'),
    ([0, 3, 3, 3], 'J80070856000'),
])
def test_check_level_cls_ALL_0_AND_1_OR_0_AND_3(dfs, values,
                                                expected_id):
    """All cells are class (0 and 1) OR (0 and 3) -> Aggregate Up (assign
    parent ID to dissolve
    ID and remove from pt dataframe"""
    df_grid, df_grid_pt = dfs
    class_df = gridgran.aggregrid(df_grid_pt, CLASSIFICATION_DICT,
                                  level='ID500m')
    for index, i in enumerate(values):
        class_df.classification.at[index] = i
    df_grid, df_grid_pt = gridgran.check_cells(class_df, df_grid, df_grid_pt,
                                               'ID500m', 'ID1000m',
                                               'ID250m', CLASSIFICATION_DICT)
    assert np.all(df_grid.dissolve_id == expected_id)


@pytest.mark.parametrize("values", [
    ([0, 1, 3, 4]),
    ([0, 1, 1, 4]),
    ([0, 3, 3, 4]),
    ([0, 0, 3, 4]),
    ([1, 4, 4, 4]),
    ([0, 4, 0, 3]),
    ([0, 1, 1, 3]),
    ([1, 1, 1, 3]),
])
def test_check_level_cls_in_0_1_3_4(dfs, values, mocker):
    """Test to check that classes that qualify for shuffling are passed to
    the shuffle_values function. THIS TEST ONLY CHECKS IF THE SHUFFLE_VALUES FUNCTION IS CALLED. THE ACTUAL FUNCTIONALITY IS TESTED IN TEST_SHUFFLE_VALUES.PY"""
    df_grid, df_grid_pt = dfs
    class_df = gridgran.aggregrid(df_grid_pt, CLASSIFICATION_DICT,
                                  level='ID500m')
    mocked = mocker.patch('gridgran.top_down_checks.shuffle_values',
                          wraps=gridgran.top_down_checks.shuffle_values)
    for index, i in enumerate(values):
        class_df.classification.at[index] = i
    df_grid, df_grid_pt = dfs
    df_grid, df_grid_pt = gridgran.check_cells(class_df, df_grid, df_grid_pt,
                                               'ID500m', 'ID1000m',
                                               'ID250m', CLASSIFICATION_DICT)
    assert mocked.called

