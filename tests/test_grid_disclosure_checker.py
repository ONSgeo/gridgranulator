from pathlib import Path

import numpy as np
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

CLASSIFICATION_SETTINGS = {
    "classification_dict": CLASSIFICATION_DICT,
    "cls_2_threshold_1000m": True,
    "cls_2_threshold_500m": True,
    "cls_2_threshold_250m": True,
    "cls_2_threshold_125m": True

}


@pytest.fixture
def dfs():
    """Makes grid joined to points by not agrregated - i.e. RAW"""
    df_grid, df_grid_pt = gridgran.prep_points_and_grid_dataframes(
        gpkg,
        CLASSIFICATION_DICT)
    df = gridgran.aggregrid(df_grid_pt, CLASSIFICATION_DICT, level='ID500m')
    yield (df_grid, df_grid_pt, df)


@pytest.fixture
def grid(dfs):
    """Prep grids"""
    DF_GRID, DF_GRID_PT, DF = dfs
    x = gridgran.GridDisclosureChecker(DF,
                                       DF_GRID,
                                       DF_GRID_PT,
                                       CLASSIFICATION_SETTINGS,
                                       )
    yield x


def test_class_instantiation(grid):
    """Test class instantiation"""
    assert isinstance(grid, gridgran.GridDisclosureChecker)


def test_execute(grid):
    """Test execution"""
    grid_final, point_final = grid.execute()
    grid_final.to_csv(BASE.joinpath('GRID.csv'), index=False)
    point_final.to_csv(BASE.joinpath('POINT.csv'), index=False)
    assert len(grid_final) == len(grid.df_grid)
    assert grid_final.p.sum() == grid.df_grid.p.sum()
    assert grid_final.dissolve_id.all() != np.nan
