from pathlib import Path

import pytest

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

LEVELS = {
    "ID1000m": {
        'child': "ID500m",
        'parent': None
    },
    "ID500m": {
        'child': "ID250m",
        'parent': "ID1000m"
    },
    "ID250m": {
        'child': "ID125m",
        'parent': "ID500m"
    },
    "ID125m": {
        'child': None,
        'parent': "ID250m"
    }
}


@pytest.fixture
def dfs():
    """Makes grid joined to points by not agrregated - i.e. RAW"""
    df_grid, df_grid_pt = gridgran.prep_points_and_grid_dataframes(
        gpkg, CLASSIFICATION_DICT)
    df = gridgran.aggregrid(df_grid_pt, CLASSIFICATION_DICT, level='ID500m')
    yield (df_grid, df_grid_pt, df)


def test_looping_through_levels(dfs):
    df_grid, df_grid_pt, df = dfs
    df_grid_500, df_grid_pt_500 = gridgran.check_cells(df, df_grid,
                                                       df_grid_pt, "ID500m",
                                                       "ID1000m", "ID250m",
                                                       CLASSIFICATION_DICT, )
    df_agg_500 = gridgran.aggregrid(df_grid_pt_500, CLASSIFICATION_DICT,
                                    level="ID500m")
    for id in df_agg_500.ID500m.to_list():
        df_ = gridgran.aggregrid(df_grid_pt_500[df_grid_pt_500.ID500m ==
                                                id], CLASSIFICATION_DICT,
                                 level="ID250m")
        df_grid_ = df_grid[df_grid.ID250m.isin(df_.ID250m.to_list())]
        df_grid_250, df_grid_pt_250 = gridgran.check_cells(df_, df_grid_,
                                                           df_grid_pt,
                                                           "ID250m", "ID500m",
                                                           "ID125m",
                                                           CLASSIFICATION_DICT, )
        df_agg_250 = gridgran.aggregrid(df_grid_pt_250, CLASSIFICATION_DICT,
                                        level="ID250m")
        assert len(df_agg_250) == 16
