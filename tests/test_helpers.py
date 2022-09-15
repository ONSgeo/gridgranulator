"""Unit tests for helpers.py"""

from pathlib import Path
from types import SimpleNamespace

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

import gridgran

BASE = Path(__file__).resolve().parent.joinpath('data')

cells_gdf = gpd.read_file(BASE.joinpath('cells_125_clip.shp')).to_crs(27700)
bfc_gdf = gpd.read_file(BASE.joinpath('BFC_clip.shp'))#.to_crs(27700)
gpkg = BASE.joinpath('GRID_1km_SUBSET.gpkg')
point = gpd.read_file(gpkg, layer='points')
grid_125m = gpd.read_file(gpkg, layer='125m')
grid_pt = pd.read_csv(BASE.joinpath('POINT.csv'))

classification_dict = {
    'p_1': 10,
    'p_2': 40,
    'p_3': 49,
    'h_1': 5,
    'h_2': 20,
    'h_3': 24,
}


@pytest.fixture
def grid_final():
    grid_final = gpd.read_file(gpkg, layer='test_grid_processed')
    yield grid_final


def test_remove_water_cells():
    gdf_125_land = gridgran.remove_water_cells(cells_gdf, bfc_gdf)
    water_cells = ["J80068221221", "J80068221421", "J80068221411",
                   "J80068221321", "J80068221311"]
    assert len(gdf_125_land) < len(cells_gdf)
    for cell in water_cells:
        assert cell not in gdf_125_land.GridID125m


def test_remove_land_cells():
    gdf_125_water = gridgran.remove_water_cells(cells_gdf, bfc_gdf,
                                                return_water=True)
    assert len(gdf_125_water) == 1


def test_calculate_dist_point_moved():
    gdf_pt = gridgran.calculate_dist_point_moved(grid_pt, point, grid_125m)
    assert 'dist_moved' in gdf_pt.columns


@pytest.mark.parametrize('p, h, above_threshold_expected', [
    (51, 25, False),
    (51, 26, True),
    (100, 0, False),
    (100, 100, True),
    (0, 0, False),
    (5, 26, False),
])
def test_check_threshold(p, h, above_threshold_expected):
    row = SimpleNamespace()
    row.p = p
    row.h = h
    threshold_p = 50
    threshold_h = 25
    above_threshold = gridgran.check_threshold(row, threshold_p, threshold_h)
    assert above_threshold == above_threshold_expected


def test_check_for_below_threshold_MAKE_MIN(grid_final):
    grid_final.loc[0:3, 'p'] = 2
    grid_final.loc[4:6, 'h'] = 2
    grid_final.loc[7:10, ['p', 'h']] = 2
    grid_final = gridgran.check_for_below_threshold(grid_final,
                                                    50,
                                                    25,
                                                    replace_with='minimum')
    assert 'above_threshold' in grid_final.columns
    assert not np.all(grid_final.loc[0:10, 'above_threshold'])
    assert np.all(grid_final.p > 50)
    assert np.all(grid_final.h > 25)


def test_check_for_below_threshold_MAKE_NULL(grid_final):
    grid_final.loc[0:3, 'p'] = 2
    grid_final.loc[4:6, 'h'] = 2
    grid_final.loc[7:10, ['p', 'h']] = 2
    grid_final = gridgran.check_for_below_threshold(grid_final,
                                                    50,
                                                    25,
                                                    replace_with='null')
    assert np.isnan(grid_final.loc[0:3, 'p']).all()
    assert np.isnan(grid_final.loc[4:6, 'h']).all()
    assert np.isnan(grid_final.loc[7:10, 'p']).all()
    assert np.isnan(grid_final.loc[7:10, 'h']).all()


def test_check_for_below_threshold_MAKE_STAR(grid_final):
    grid_final.loc[0:3, 'p'] = 2
    grid_final.loc[4:6, 'h'] = 2
    grid_final.loc[7:10, ['p', 'h']] = 2
    grid_final = gridgran.check_for_below_threshold(grid_final,
                                                    50,
                                                    25,
                                                    replace_with='star')
    assert np.all(grid_final.loc[0:3, 'p'] == "*")
    assert np.all(grid_final.loc[4:6, 'h'] == "*")
    assert np.all(grid_final.loc[7:10, 'p'] == "*")
    assert np.all(grid_final.loc[7:10, 'h'] == "*")


def test_make_point_df_removing_grids():
    pt_df = pd.read_csv(BASE.joinpath("AGGR_PTS_test.csv"))
    print(pt_df)
    pt_df_points_only = gridgran.make_point_df_removing_grids(pt_df)
    print(pt_df_points_only)
    assert len(pt_df) > len(pt_df_points_only)
    assert np.all(pt_df_points_only.uprn != np.nan)


def test_remove_duplicates():
    gdf_1000 = gpd.read_file(BASE.joinpath('GRIDS.gpkg'), layer='1000m')
    points = gpd.read_file(BASE.joinpath('GRIDS.gpkg'), layer='points')
    df_grid_pt = gdf_1000.to_crs(27700).sjoin(points, how='left',
                                              predicate='intersects')
    df_grid_removed = gridgran.remove_duplicates(df_grid_pt)
    array_orig = df_grid_pt.uprn.dropna()
    array_dups_rm = df_grid_removed.uprn.dropna()
    assert not np.all(array_orig.duplicated() == False)
    assert np.all(array_dups_rm.duplicated() == False)
    assert df_grid_removed.people.sum() == points.people.sum()
