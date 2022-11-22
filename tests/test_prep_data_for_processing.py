"""Unit tests for gridgran.prep_data_for_processing"""
from pathlib import Path

import geopandas as gpd
import pytest
import fiona

import gridgran

BASE = Path(__file__).resolve().parent.joinpath('data')
GPKG = BASE.joinpath('Extract_points.gpkg')
LA_LAYER = 'LAs'
PT_LAYER = 'POINTS_GLOBAL'

# This path should be relative from the user's computer
GRIDS_125m = Path(r'R:\HeatherPorter\CensusGrids\Nested '
                  r'Grids\NestedGridData\UKGrids\UKGrid_125m.gpkg').resolve()


@pytest.mark.parametrize('la_ids', [
    (['E06000030', 'E06000036', 'E06000037']),
    (['E07000092', 'E07000093', 'E07000094', 'E07000179', 'E07000180',
      'E07000209', 'E07000214']),
    (['E06000059'])
])
def test_get_la_geoms(la_ids):
    la_path = GPKG
    layer = LA_LAYER
    la_col = 'LAD21CD'
    gdf = gridgran.get_la_geoms(la_path, la_ids, la_col, layer=layer)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == len(la_ids)
    assert sorted(gdf[la_col].tolist()) == sorted(la_ids)
    assert list(gdf.columns) == [la_col, 'geometry']


@pytest.mark.parametrize('la_ids', [
    (['Windsor and Maidenhead', 'Horsham', 'Reading']),
    (['Swindon'])
])
def test_get_la_geoms_using_la_names(la_ids):
    la_path = GPKG
    layer = LA_LAYER
    la_col = 'LAD21NM'
    gdf = gridgran.get_la_geoms(la_path, la_ids, la_col, layer=layer)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == len(la_ids)
    assert sorted(gdf[la_col].tolist()) == sorted(la_ids)
    assert list(gdf.columns) == [la_col, 'geometry']


@pytest.mark.parametrize('la_ids, expect_no_pts', [
    (['E06000059'], 1594),
    (['E06000030', 'E06000037', 'E06000054', 'E07000180'], 3944),
    (['E07000084', 'E07000085', 'E07000094', 'E07000089', 'E07000216',
      'E07000225', 'E07000209'], 3498)
])
def test_get_points(la_ids, expect_no_pts):
    gdf = gridgran.get_la_geoms(GPKG, la_ids, 'LAD21CD', layer=LA_LAYER)
    pts = gridgran.get_points(gdf, GPKG, layer=PT_LAYER)
    assert isinstance(pts, gpd.GeoDataFrame)
    assert len(pts) == expect_no_pts


# This test is commented out because it can't be run on github (needs
# network connection). It can be reinstated
# @pytest.mark.skip(reason='Needs to read large data from network - SLOW')
# def test_get_grids():
#     gdf = gridgran.get_la_geoms(GPKG, ['E07000217'], 'LAD21CD',
#     layer=LA_LAYER)
#     pts = gridgran.get_points(gdf, GPKG, layer=PT_LAYER)
#     grid_1km, grid_125m = gridgran.get_grids(
#         pts,
#         GPKG,
#         GRIDS_125m,
#         layer_1km='GLOBAL_1km',
#         layer_125m=None
#     )
#     assert len(grid_125m) == len(grid_1km) * 64
#     assert isinstance(grid_125m, gpd.GeoDataFrame)
#     assert isinstance(grid_1km, gpd.GeoDataFrame)

# This test is commented out because it can't be run on github (needs
# network connection). It can be reinstated but check the path to GRIDS_125m
# relative to the tester's computer
# def test_make_geopackage():
#     gridgran.make_points_geopackage(
#         GPKG,
#         ['Woking'],
#         'LAD21NM',
#         GPKG,
#         GPKG,
#         GPKG,
#         GRIDS_125m,
#         out_layer='points',
#         la_layer=LA_LAYER,
#         pt_layer=PT_LAYER,
#         pt_pop_col='people',
#         layer_1km='GLOBAL_1km',
#         layer_125m=None
#     )
#     assert 'points' in fiona.listlayers(GPKG)
#     assert '1000m' in fiona.listlayers(GPKG)
#     assert '125m' in fiona.listlayers(GPKG)
