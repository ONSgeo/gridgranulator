"""Unit tests to deal solely with moving rows around in the shuffle_values
function in top_down_checks"""
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pytest_mock import mocker

import gridgran

# TODO: When [1,3] OR [0, 1, 3] -> Sum up everything and check the
#  total/num_3s_in_cell >= Threshold
# TODO: Find a way to isolate rows that can be shared/shuffled

import tests

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
    """Makes grid joined to points by not agrregated - i.e. RAW"""
    df_grid, df_grid_pt = gridgran.prep_points_and_grid_dataframes(gpkg,
                                                                   CLASSIFICATION_DICT,)
    df = gridgran.aggregrid(df_grid_pt, CLASSIFICATION_DICT, level='ID500m')
    yield (df_grid, df_grid_pt, df)


def test_grids_n_pts_made(dfs):
    assert isinstance(dfs[0], pd.DataFrame)
    assert isinstance(dfs[1], pd.DataFrame)
    assert len(dfs[2]) == 4

@pytest.mark.parametrize("cell_configs", [
    ([1,2,3,4]),
    ([0,1,3,4]),
    ([1,1,1,1]),
    ([3,3,1,1]),
    ([1,4,2,3]),
])
def test_make_any_combination(dfs, cell_configs):
    df_500 = dfs[2]
    df, df_pt, df_GLOBAL = tests.make_any_combination(df_500, dfs[1], dfs[0],
                                           cell_configs,
                         upper_lower='upper')
    assert df.p.sum() == df_GLOBAL.p.sum()
    assert df.classification.to_list() == cell_configs

@pytest.mark.parametrize("cell_configs", [
    ([4,4,4,4]),
    ([1,2,3,4]),
    ([0,1,3,4]),
    ([4,1,4,1]),
    ([3,3,1,1]),
    ([1,4,2,3]),
])
def test_remake_cls_4(dfs, cell_configs):
    df_500 = dfs[2]
    df, df_pt, df_GLOBAL = tests.make_any_combination(df_500, dfs[1], dfs[0], cell_configs,
                         upper_lower='upper', remake_cls_4=True)
    assert df.classification.to_list() == cell_configs

@pytest.mark.parametrize("cell_configs", [
    ([0, 1, 3, 4]),
    ([1, 3, 1, 3]),
    ([1, 1, 1, 4]),
    ([3, 3, 4, 4]),
    ([0, 1, 3, 3]),
    ([0, 0, 1, 4]),
    ([0, 3, 3, 4]),
    ([1 ,3 ,4, 4])
])
def test_shuffle_values_called(dfs, cell_configs, mocker):
    df_500 = dfs[2]
    mocked = mocker.patch('gridgran.top_down_checks.shuffle_values',
                          wraps=gridgran.top_down_checks.shuffle_values)
    df, df_pt, df_GLOBAL = tests.make_any_combination(df_500, dfs[1], dfs[0], cell_configs,
                                           upper_lower='upper',
                                           remake_cls_4=True)
    df_grid, df_grid_pt = gridgran.check_cells(df, df_GLOBAL, dfs[1],
                                               'ID500m', 'ID1000m',
                                               'ID250m', CLASSIFICATION_DICT,)
    assert mocked.called

@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 3]),
    ([1, 3, 3, 3]),
    ([1, 3, 1, 3]),
    ([0, 1, 3, 3]),
    ([0, 0, 1, 3]),
    ([1, 3, 4, 4]),
    ([1, 1, 3, 4])
])
def test_cls_3_cells_aggregated_up_when_check_cls_3_can_become_cls_4_fails(
        dfs, cell_configs):
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid,
                                                         cell_configs,
                                                         upper_lower='lower',
                                                         remake_cls_4=True)
    df_grid_checked, df_grid_pt_checked = gridgran.check_cells(DF,
                                               DF_GRID,
                                               DF_GRID_PT,
                                               'ID500m',
                                               'ID1000m',
                                               'ID250m',
                                               CLASSIFICATION_DICT,)
    df_grid_aggregated = gridgran.aggregrid(df_grid_checked,
                                            CLASSIFICATION_DICT, level="ID500m")
    assert np.all(df_grid_checked.dissolve_id == 'J80070856000')
    assert df_grid_checked.p.sum() == DF_GRID_PT.p.sum()
    assert len(df_grid_checked) == 64





