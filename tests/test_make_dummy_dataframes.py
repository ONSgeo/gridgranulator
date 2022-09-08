"""This pytest module just tests the output of the helper functions in
make_dummy_tests module """
from pathlib import Path
import pytest

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
    df_grid, df_grid_pt = gridgran.prep_points_and_grid_dataframes(
        gpkg,
        CLASSIFICATION_DICT)
    df = gridgran.aggregrid(df_grid_pt, CLASSIFICATION_DICT, level='ID500m')
    yield (df_grid, df_grid_pt, df)


def test_dfs(dfs):
    """Test dfs returned"""
    assert len(dfs[2]) == 4
    assert len(dfs[0]) == 64


def test_make_any_combination(dfs):
    """Test make_any_combination()"""
    cell_config = [1, 2, 3, 4]
    df_500m = dfs[2]
    df_pt = dfs[1]
    df_grid = dfs[0]
    df, df_pt, df_GLOBAL = tests.make_any_combination(df_500m, df_pt, df_grid,
                                                      cell_config,
                                                      )
    assert len(df_pt.ID125m.unique()) == 64
    assert len(df_GLOBAL.ID125m.unique()) == 64
    assert len(df.ID500m.unique()) == 4
    assert df.classification.to_list() == cell_config


@pytest.mark.parametrize("cell_config", [
    ([1, 1, 1, 4]),
    ([0, 0, 0, 0]),
    ([1, 2, 3, 4]),
    ([4, 4, 4, 4]),
    ([0, 0, 0, 0]),
    ([0, 0, 0, 1]),
    ([0, 0, 0, 2]), ([0, 0, 0, 3]), ([0, 0, 0, 4]), ([0, 0, 1, 1]),
    ([0, 0, 1, 2]), ([0, 0, 1, 3]), ([0, 0, 1, 4]), ([0, 0, 2, 2]),
    ([0, 0, 2, 3]), ([0, 0, 2, 4]), ([0, 0, 3, 3]), ([0, 0, 3, 4]),
    ([0, 0, 4, 4]), ([0, 1, 1, 1]), ([0, 1, 1, 2]), ([0, 1, 1, 3]),
    ([0, 1, 1, 4]), ([0, 1, 2, 2]), ([0, 1, 2, 3]), ([0, 1, 2, 4]),
    ([0, 1, 3, 3]), ([0, 1, 3, 4]), ([0, 1, 4, 4]), ([0, 2, 2, 2]),
    ([0, 2, 2, 3]), ([0, 2, 2, 4]), ([0, 2, 3, 3]), ([0, 2, 3, 4]),
    ([0, 2, 4, 4]), ([0, 3, 3, 3]), ([0, 3, 3, 4]), ([0, 3, 4, 4]),
    ([0, 4, 4, 4]), ([1, 1, 1, 1]), ([1, 1, 1, 2]), ([1, 1, 1, 3]),
    ([1, 1, 1, 4]), ([1, 1, 2, 2]), ([1, 1, 2, 3]), ([1, 1, 2, 4]),
    ([1, 1, 3, 3]), ([1, 1, 3, 4]), ([1, 1, 4, 4]), ([1, 2, 2, 2]),
    ([1, 2, 2, 3]), ([1, 2, 2, 4]), ([1, 2, 3, 3]), ([1, 2, 3, 4]),
    ([1, 2, 4, 4]), ([1, 3, 3, 3]), ([1, 3, 3, 4]), ([1, 3, 4, 4]),
    ([1, 4, 4, 4]), ([2, 2, 2, 2]), ([2, 2, 2, 3]), ([2, 2, 2, 4]),
    ([2, 2, 3, 3]), ([2, 2, 3, 4]), ([2, 2, 4, 4]), ([2, 3, 3, 3]),
    ([2, 3, 3, 4]), ([2, 3, 4, 4]), ([2, 4, 4, 4]), ([3, 3, 3, 3]),
    ([3, 3, 3, 4]), ([3, 3, 4, 4]), ([3, 4, 4, 4]), ([4, 4, 4, 4])
])
def test_simple_all_combinations(dfs, cell_config):
    """Try to make dummy dataframes len(4) with each of the above test case
    scenarios"""
    df_500m = dfs[2]
    df_pt = dfs[1]
    df_grid = dfs[0]
    df, df_pt, df_GLOBAL = tests.make_any_combination(df_500m, df_pt, df_grid,
                                                      cell_config,
                                                      )
    assert len(df_pt.ID125m.unique()) == 64
    assert len(df_GLOBAL.ID125m.unique()) == 64
    assert len(df.ID500m.unique()) == 4


@pytest.mark.parametrize("cell_config", [
    ([1, 2, 3, 4]),
    ([4, 4, 4, 4]),
    ([0, 0, 0, 0]),
    ([0, 0, 0, 1]),
    ([0, 0, 0, 2]), ([0, 0, 0, 3]), ([0, 0, 0, 4]), ([0, 0, 1, 1]),
    ([0, 0, 1, 2]), ([0, 0, 1, 3]), ([0, 0, 1, 4]), ([0, 0, 2, 2]),
    ([0, 0, 2, 3]), ([0, 0, 2, 4]), ([0, 0, 3, 3]), ([0, 0, 3, 4]),
    ([0, 0, 4, 4]), ([0, 1, 1, 1]), ([0, 1, 1, 2]), ([0, 1, 1, 3]),
    ([0, 1, 1, 4]), ([0, 1, 2, 2]), ([0, 1, 2, 3]), ([0, 1, 2, 4]),
    ([0, 1, 3, 3]), ([0, 1, 3, 4]), ([0, 1, 4, 4]), ([0, 2, 2, 2]),
    ([0, 2, 2, 3]), ([0, 2, 2, 4]), ([0, 2, 3, 3]), ([0, 2, 3, 4]),
    ([0, 2, 4, 4]), ([0, 3, 3, 3]), ([0, 3, 3, 4]), ([0, 3, 4, 4]),
    ([0, 4, 4, 4]), ([1, 1, 1, 1]), ([1, 1, 1, 2]), ([1, 1, 1, 3]),
    ([1, 1, 1, 4]), ([1, 1, 2, 2]), ([1, 1, 2, 3]), ([1, 1, 2, 4]),
    ([1, 1, 3, 3]), ([1, 1, 3, 4]), ([1, 1, 4, 4]), ([1, 2, 2, 2]),
    ([1, 2, 2, 3]), ([1, 2, 2, 4]), ([1, 2, 3, 3]), ([1, 2, 3, 4]),
    ([1, 2, 4, 4]), ([1, 3, 3, 3]), ([1, 3, 3, 4]), ([1, 3, 4, 4]),
    ([1, 4, 4, 4]), ([2, 2, 2, 2]), ([2, 2, 2, 3]), ([2, 2, 2, 4]),
    ([2, 2, 3, 3]), ([2, 2, 3, 4]), ([2, 2, 4, 4]), ([2, 3, 3, 3]),
    ([2, 3, 3, 4]), ([2, 3, 4, 4]), ([2, 4, 4, 4]), ([3, 3, 3, 3]),
    ([3, 3, 3, 4]), ([3, 3, 4, 4]), ([3, 4, 4, 4]), ([4, 4, 4, 4])
])
def test_all_combinations(dfs, cell_config):
    """Try to make dummy dataframes len(4) with each of the above test case
    scenarios"""
    df_500m = dfs[2]
    df_pt = dfs[1]
    df_grid = dfs[0]
    df, df_pt, df_GLOBAL = tests.make_any_combination(df_500m, df_pt, df_grid,
                                                      cell_config,
                                                      upper_lower='lower',
                                                      )
    assert len(df_pt.ID125m.unique()) == 64
    assert len(df_GLOBAL.ID125m.unique()) == 64
    assert len(df.ID500m.unique()) == 4
    assert df.p.sum() == df_GLOBAL.p.sum()
    assert df.p.sum() == df_pt.p.sum()
    assert df.classification.to_list() == cell_config
