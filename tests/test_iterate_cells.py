from pathlib import Path

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from pytest_mock import mocker

import gridgran

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
    df_grid, df_grid_pt = gridgran.prep_points_and_grid_dataframes(gpkg, CLASSIFICATION_DICT)
    df = gridgran.aggregrid(df_grid_pt, CLASSIFICATION_DICT, level='ID500m')
    yield (df_grid, df_grid_pt, df)

@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 4]),
    ([0, 0, 1, 4]),
    ([1, 0, 4, 4]),
    ([0, 0, 1, 4]),
    ([4, 4, 4, 1]),
])
def test_check_cells_children_are_valid_PASS(dfs, cell_configs):
    DF_GRID, DF_GRID_PT, DF = dfs
    df, df_pt, df_grid = tests.make_any_combination(DF, DF_GRID_PT,
                                                    DF_GRID, cell_configs)
    df_grid_checked, df_grid_pt_checked, df_checked, child_cells_valid = \
        gridgran.check_cells_children_are_valid(df,
                                                df_grid,
                                                df_pt,
                                                "ID500m",
                                                "ID1000m",
                                                "ID250m",
                                                CLASSIFICATION_DICT)
    assert len(df_checked) == 4
    assert np.in1d(df_checked.classification.unique().all(), [0, 4])
    assert child_cells_valid

@pytest.mark.parametrize("cell_configs", [
    ([1, 2, 1, 4]),
    ([0, 0, 3, 0]),
    ([2, 0, 3, 1]),
    ([4, 3, 1, 3]),
    ([1, 2, 3, 4]),
])
def test_check_cells_children_are_valid_FAIL(dfs, cell_configs):
    DF_GRID, DF_GRID_PT, DF = dfs
    df, df_pt, df_grid = tests.make_any_combination(DF, DF_GRID_PT,
                                                    DF_GRID, cell_configs,
                                                    upper_lower="lower",
                                                    remake_cls_4=True)
    df_grid_checked, df_grid_pt_checked, df_checked, child_cells_valid = \
        gridgran.check_cells_children_are_valid(df,
                                                df_grid,
                                                df_pt,
                                                "ID500m",
                                                "ID1000m",
                                                "ID250m",
                                                CLASSIFICATION_DICT)
    assert len(df_checked) == 4
    assert np.in1d(df_checked.classification.unique().all(), [0, 1, 2, 3, 4])
    assert not child_cells_valid

@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 4]),
    ([0, 0, 1, 4]),
    ([1, 0, 4, 4]),
    ([0, 0, 1, 4]),
    ([4, 4, 4, 1]),
])
def test_get_children_ids(dfs, cell_configs):
    DF_GRID, DF_GRID_PT, DF = dfs
    df, df_pt, df_grid = tests.make_any_combination(DF, DF_GRID_PT,
                                                    DF_GRID, cell_configs,
                                                    upper_lower="upper",
                                                    remake_cls_4=False)
    children_ids = gridgran.get_children_ids(df, 'ID500m')
    assert len(children_ids) == 4
    assert isinstance(children_ids, list)
    assert children_ids == list(df['ID500m'].unique())

@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 4]),
    ([0, 0, 1, 4]),
    ([1, 0, 4, 4]),
    ([0, 0, 1, 4]),
    ([4, 4, 4, 1]),
])
def test_subset_by_id(dfs, cell_configs):
    DF_GRID, DF_GRID_PT, DF = dfs
    df, df_pt, df_grid = tests.make_any_combination(DF, DF_GRID_PT,
                                                    DF_GRID, cell_configs,
                                                    upper_lower="upper",
                                                    remake_cls_4=False)
    df_grid_checked, df_grid_pt_checked, df_checked, child_cells_valid = \
        gridgran.check_cells_children_are_valid(df,
                                                df_grid,
                                                df_pt,
                                                "ID500m",
                                                "ID1000m",
                                                "ID250m",
                                                CLASSIFICATION_DICT,)
    for i in gridgran.get_children_ids(df_checked, 'ID500m'):
        df_grid_subset, df_grid_pt_subset, df_subset = \
            gridgran.subset_by_id(df_grid_checked, df_grid_pt_checked,
                                  "ID500m", "ID250m", i, CLASSIFICATION_DICT)
        assert list(df_grid_subset.ID500m.unique()) == [i]
        assert list(df_grid_pt_subset.ID500m.unique()) == [i]
        assert len(df_subset) == 4

@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 4]),
    ([0, 0, 1, 4]),
    ([1, 0, 4, 4]),
    ([0, 0, 1, 4]),
    ([4, 4, 4, 1]),
])
def test_subset_by_id_for_ID125m(dfs, cell_configs):
    DF_GRID, DF_GRID_PT, DF = dfs
    df, df_pt, df_grid = tests.make_any_combination(DF, DF_GRID_PT,
                                                    DF_GRID, cell_configs,
                                                    upper_lower="upper",
                                                    remake_cls_4=False)
    df = gridgran.aggregrid(df_pt, CLASSIFICATION_DICT, level='ID125m')
    df_grid_checked, df_grid_pt_checked, df_checked, child_cells_valid = \
        gridgran.check_cells_children_are_valid(df,
                                                df_grid,
                                                df_pt,
                                                "ID125m",
                                                "ID250m",
                                                "ID125m",
                                                CLASSIFICATION_DICT)
    for i in gridgran.get_children_ids(df_checked, 'ID125m'):
        df_grid_subset, df_grid_pt_subset, df_subset = \
            gridgran.subset_by_id(df_grid_checked, df_grid_pt_checked,
                                  "ID125m", "ID125m", i, CLASSIFICATION_DICT)
        assert list(df_grid_subset.ID125m.unique()) == [i]
        assert list(df_grid_pt_subset.ID125m.unique()) == [i]
        assert len(df_subset) == 1

