from pathlib import Path

import numpy as np
from pandas.testing import assert_frame_equal
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
        gpkg, CLASSIFICATION_DICT)
    df = gridgran.aggregrid(df_grid_pt, CLASSIFICATION_DICT, level='ID500m')
    yield (df_grid, df_grid_pt, df)


@pytest.mark.parametrize("cell_configs", [
    ([0, 0, 0, 0]),
    ([0, 0, 0, 1]),
    ([0, 0, 0, 2]),
    ([0, 0, 0, 3]),
    ([0, 0, 0, 4]),
    ([0, 0, 1, 1]),
    ([0, 0, 1, 2]),
    ([0, 0, 1, 3]),
    ([0, 0, 1, 4]),
    ([0, 0, 2, 2]),
    ([0, 0, 2, 3]),
    ([0, 0, 2, 4]),
    ([0, 0, 3, 3]),
    ([0, 0, 3, 4]),
    ([0, 0, 4, 4]),
    ([0, 1, 1, 1]),
    ([0, 1, 1, 2]),
    ([0, 1, 1, 3]),
    ([0, 1, 1, 4]),
    ([0, 1, 2, 2]),
    ([0, 1, 2, 3]),
    ([0, 1, 2, 4]),
    ([0, 1, 3, 3]),
    ([0, 1, 3, 4]),
    ([0, 1, 4, 4]),
    ([0, 2, 2, 2]),
    ([0, 2, 2, 3]),
    ([0, 2, 2, 4]),
    ([0, 2, 3, 3]),
    ([0, 2, 3, 4]),
    ([0, 2, 4, 4]),
    ([0, 3, 3, 3]),
    ([0, 3, 3, 4]),
    ([0, 3, 4, 4]),
    ([0, 4, 4, 4]),
    ([1, 1, 1, 1]),
    ([1, 1, 1, 2]),
    ([1, 1, 1, 3]),
    ([1, 1, 1, 4]),
    ([1, 1, 2, 2]),
    ([1, 1, 2, 3]),
    ([1, 1, 2, 4]),
    ([1, 1, 3, 3]),
    ([1, 1, 3, 4]),
    ([1, 1, 4, 4]),
    ([1, 2, 2, 2]),
    ([1, 2, 2, 3]),
    ([1, 2, 2, 4]),
    ([1, 2, 3, 3]),
    ([1, 2, 3, 4]),
    ([1, 2, 4, 4]),
    ([1, 3, 3, 3]),
    ([1, 3, 3, 4]),
    ([1, 3, 4, 4]),
    ([1, 4, 4, 4]),
    ([2, 2, 2, 2]),
    ([2, 2, 2, 3]),
    ([2, 2, 2, 4]),
    ([2, 2, 3, 3]),
    ([2, 2, 3, 4]),
    ([2, 2, 4, 4]),
    ([2, 3, 3, 3]),
    ([2, 3, 3, 4]),
    ([2, 3, 4, 4]),
    ([2, 4, 4, 4]),
    ([3, 3, 3, 3]),
    ([3, 3, 3, 4]),
    ([3, 3, 4, 4]),
    ([3, 4, 4, 4]),
    ([4, 4, 4, 4]),
])
def test_int_single_level(dfs, cell_configs):
    DF_GRID, DF_GRID_PT, DF = dfs
    df, df_pt, df_grid = tests.make_any_combination(DF,
                                                    DF_GRID_PT,
                                                    DF_GRID,
                                                    cell_configs,
                                                    upper_lower="upper")
    df_grid_checked, df_grid_pt_checked = gridgran.check_cells(
        df,
        df_grid,
        df_pt,
        'ID500m',
        'ID1000m',
        'ID250m',
        CLASSIFICATION_DICT, )

    if np.any(cell_configs == 2) or np.all(cell_configs == 1) or \
            np.all(cell_configs == 3) or \
            sorted(list(np.unique(cell_configs))) == [0, 1] or \
            sorted(list(np.unique(cell_configs))) == [0, 3]:  # Aggregate up
        assert not df_grid_checked.dissolve_id.isnull().values.any()
    elif np.all(cell_configs == 4) or np.all(cell_configs == 0) or np.all(
            np.unique(cell_configs) == [0, 4]):
        assert_frame_equal(df_grid_pt_checked, df_pt)
        assert_frame_equal(df_grid_checked, df_grid)

    assert len(df_grid_checked) == 64
    assert df_grid_checked.p.sum() == df_pt.p.sum()
    df_agg = gridgran.aggregrid(df_grid_checked, CLASSIFICATION_DICT,
                                level="ID500m")
    assert [x in [0, 4] for x in list(df_agg.classification.unique())]
    assert [x not in [1, 2, 3] for x in list(df_agg.classification.unique())]


def test_ind_configurations(dfs):
    cell_configs = [0, 0, 1, 4]
    DF_GRID, DF_GRID_PT, DF = dfs
    df, df_pt, df_grid = tests.make_any_combination(DF,
                                                    DF_GRID_PT,
                                                    DF_GRID,
                                                    cell_configs,
                                                    upper_lower="upper")
    df_grid_checked, df_grid_pt_checked = gridgran.check_cells(
        df,
        df_grid,
        df_pt,
        'ID500m',
        'ID1000m',
        'ID250m',
        CLASSIFICATION_DICT, )
    assert not np.all(df_grid_pt_checked.ID500m_LEVEL_MOVE_ORIGIN == np.nan)
    assert np.all(df_grid_pt_checked.ID250m_LEVEL_MOVE_ORIGIN.isna())
    assert len(df_grid_checked) == 64
    assert df_grid_pt_checked.p.sum() == df_pt.p.sum()

    if np.any(cell_configs == 2) or np.all(cell_configs == 1) or \
            np.all(cell_configs == 3) or \
            sorted(list(np.unique(cell_configs))) == [0, 1] or \
            sorted(list(np.unique(cell_configs))) == [0, 3]:  # Aggregate up
        assert not df_grid_checked.dissolve_id.isnull().values.any()
    elif np.all(cell_configs == 4) or np.all(cell_configs == 0) or np.all(
            np.unique(cell_configs) == [0, 4]):
        assert_frame_equal(df_grid_pt_checked, df_pt)
        assert_frame_equal(df_grid_checked, df_grid)
    assert len(df_grid_checked) == 64
    assert df_grid_pt_checked.p.sum() == df_pt.p.sum()
    df_agg = gridgran.aggregrid(df_grid_checked, CLASSIFICATION_DICT,
                                level="ID500m")
    assert [x in [0, 4] for x in list(df_agg.classification.unique())]
    assert [x not in [1, 2, 3] for x in list(df_agg.classification.unique())]
