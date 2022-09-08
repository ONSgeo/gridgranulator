"""Unit tests for shuffle_helpers"""
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
        'p_3': 49,
        'h_1': 5,
        'h_2': 20,
        'h_3': 24,
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
def test_shuffle_values_0_1_4(dfs, cell_configs):
    DF_GRID, DF_GRID_PT, DF = dfs
    df, df_pt, df_grid = tests.make_any_combination(DF, DF_GRID_PT,
                                                    DF_GRID, cell_configs)
    parent_index = df.ID1000m[0]
    df_grid, df_grid_pt = gridgran.shuffle_values(df, df_grid, df_pt, \
                                                  "ID500m", "ID1000m",
                                                  "ID250m", parent_index,
                                                  CLASSIFICATION_DICT,)
    DF_MOVED = gridgran.aggregrid(df_grid_pt, CLASSIFICATION_DICT,
                                  level='ID500m',
                                  template=True)
    assert len(df_grid.ID125m.unique()) == 64
    assert len(df_grid_pt.ID125m.unique()) == 64
    assert df_grid.p.sum() == df.p.sum()
    assert df_grid_pt.p.sum() == df_pt.p.sum()
    assert DF_MOVED.p.sum() == df_grid_pt.p.sum()
    assert len(DF_MOVED.ID500m.unique()) == 4
    assert not 1 in DF_MOVED.classification.to_list()
    assert 0 in DF_MOVED.classification.to_list()
    assert 4 in DF_MOVED.classification.to_list()


@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 4]),
    ([0, 0, 1, 4]),
    ([1, 0, 4, 4]),
    ([0, 0, 1, 4]),
    ([4, 4, 4, 1]),
])
def test_move_cls_1_to_4(dfs, cell_configs):
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid, cell_configs)
    df_grid_moved, df_grid_pt_moved = gridgran.move_cls_1_to_4(DF, DF_GRID,
                                                               DF_GRID_PT,
                                                               "ID500m",
                                                               "ID1000m",
                                                               "ID250m",
                                                               CLASSIFICATION_DICT)
    DF_MOVED = gridgran.aggregrid(df_grid_pt_moved, CLASSIFICATION_DICT,
                                  level='ID500m',
                                  template=True)
    assert len(df_grid_moved.ID125m.unique()) == 64
    assert len(df_grid_pt_moved.ID125m.unique()) == 64
    assert df_grid_moved.p.sum() == DF_GRID.p.sum()
    assert df_grid_pt_moved.p.sum() == DF_GRID_PT.p.sum()
    assert DF_MOVED.p.sum() == df_grid_pt_moved.p.sum()
    assert len(DF_MOVED.ID500m.unique()) == 4
    assert not 1 in DF_MOVED.classification.to_list()
    assert 0 in DF_MOVED.classification.to_list()
    assert 4 in DF_MOVED.classification.to_list()


@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 4]),
    ([0, 0, 1, 4]),
    ([1, 0, 4, 4]),
    ([0, 0, 1, 4]),
    ([4, 4, 4, 1]),
])
def test_move_cls_1_to_4_from_check_cells(dfs, cell_configs):
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid, cell_configs)
    df_grid_shuffled, df_pt_grid_shuffled = gridgran.check_cells(DF,
                                                                 DF_GRID,
                                                                 DF_GRID_PT,
                                                                 "ID500m",
                                                                 "ID1000m",
                                                                 "ID250m",
                                                                 CLASSIFICATION_DICT)
    DF_AGGR = gridgran.aggregrid(df_pt_grid_shuffled, CLASSIFICATION_DICT,
                                 level="ID500m",
                                 template=False)
    assert [x in cell_configs for x in list(DF_AGGR.classification.unique())]
    assert np.all(DF_AGGR.classification.unique() != 1)
    assert len(df_grid_shuffled) == 64
    assert len(df_pt_grid_shuffled.ID125m.unique()) == 64
    assert df_pt_grid_shuffled.p.sum() == DF_GRID_PT.p.sum()


@pytest.mark.parametrize("cell_configs", [
    ([0, 0, 0, 4]),
    ([0, 0, 4, 4]),
    ([1, 3, 4, 4]),
    ([4, 4, 4, 4])
])
def test_separate_excess_rows_in_df_cls_4(dfs, cell_configs):
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid, cell_configs)
    IDS = DF[DF.classification == 4].ID500m.tolist()
    for ID in IDS:
        df_pt = DF_GRID_PT[DF_GRID_PT.ID500m == ID]
        df_pt_separated, df_pt_excess = \
            gridgran.separate_excess_rows_in_df_cls_4(df_pt)
        assert len(df_pt) == len(df_pt_separated) + len(df_pt_excess)

@pytest.mark.parametrize("cell_configs", [
    ([0, 0, 0, 4]),
    ([0, 0, 4, 4]),
    ([1, 3, 4, 4]),
    ([4, 4, 4, 4])
])
def test_separated_points_and_excess_points(dfs, cell_configs):
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid, cell_configs)
    IDS = DF[DF.classification == 4].ID500m.tolist()
    for ID in IDS:
        df_pt = DF_GRID_PT[DF_GRID_PT.ID500m == ID]
        df_pt_separated, df_pt_excess = \
            gridgran.separate_excess_rows_in_df_cls_4(df_pt)

        df_pt_sep, df_pt_ex = \
        gridgran.get_separated_points_and_excess_points(
        df_pt_separated, df_pt, 50, 20)
        assert len(df_pt_sep) == len(df_pt_separated)


@pytest.mark.parametrize("cell_configs", [
    ([0, 0, 0, 4]),
    ([0, 0, 4, 4]),
    ([1, 3, 4, 4]),
    ([4, 4, 4, 4])
])
def test_make_class_4_df_pt_meet_threshold(dfs, cell_configs):
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid,
                                                         cell_configs,
                                                         upper_lower='upper')
    DF_CLASS_4 = DF[DF.classification == 4]
    ID_TO_SEPARATE = DF_CLASS_4.ID500m.to_list()[0]
    DF_PT_TO_SEPARATE = DF_GRID_PT[DF_GRID_PT.ID500m == ID_TO_SEPARATE]
    df_pt_separated, df_pt_excess = \
        gridgran.separate_excess_rows_in_df_cls_4(DF_PT_TO_SEPARATE)
    assert len(df_pt_separated) >= 25
    assert df_pt_separated.p.sum() >= 50
    assert df_pt_separated.p.sum() + df_pt_excess.p.sum() == \
           DF_PT_TO_SEPARATE.p.sum()
    assert len(df_pt_separated) + len(df_pt_excess) == len(DF_PT_TO_SEPARATE)


@pytest.mark.parametrize("cell_configs", [
    ([0, 0, 0, 4]),
    ([0, 0, 4, 4]),
    ([1, 3, 4, 4]),
    ([4, 4, 4, 4])
])
def test_lower_thresholds_make_class_4_df_pt_meet_threshold(dfs, cell_configs):
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid,
                                                         cell_configs,
                                                         upper_lower='lower')
    DF_CLASS_4 = DF[DF.classification == 4]
    ID_TO_SEPARATE = DF_CLASS_4.ID500m.to_list()[0]
    DF_PT_TO_SEPARATE = DF_GRID_PT[DF_GRID_PT.ID500m == ID_TO_SEPARATE]
    df_pt_separated, df_pt_excess = \
        gridgran.separate_excess_rows_in_df_cls_4(DF_PT_TO_SEPARATE)
    assert len(df_pt_separated) >= 25
    assert df_pt_separated.p.sum() >= 50
    assert df_pt_separated.p.sum() + df_pt_excess.p.sum() == \
           DF_PT_TO_SEPARATE.p.sum()
    assert len(df_pt_separated) + len(df_pt_excess) == len(DF_PT_TO_SEPARATE)


@pytest.mark.parametrize("cell_configs", [
    ([0, 0, 0, 4]),
    ([0, 0, 4, 4]),
    ([1, 3, 4, 4]),
    ([4, 4, 4, 4])
])
def test_make_class_4_df_pt_meet_threshold_raises_exception_when_not_enough_points(
        dfs, cell_configs):
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid,
                                                         cell_configs,
                                                         upper_lower='lower',
                                                         remake_cls_4=True)
    DF_CLASS_4 = DF[DF.classification == 4]
    ID_TO_SEPARATE = DF_CLASS_4.ID500m.to_list()[0]
    DF_PT_TO_SEPARATE = DF_GRID_PT[DF_GRID_PT.ID500m == ID_TO_SEPARATE]
    with pytest.raises(
            gridgran.DataFrameCouldNotBeSeparatedException) as exc_info:
        df_pt_separated, df_pt_excess = gridgran.separate_excess_rows_in_df_cls_4(
            DF_PT_TO_SEPARATE)
        assert exc_info.value.args[
                   0] == "DataFrame could not be separated effectively to meet threshold"


@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 3]),
    ([1, 3, 3, 3]),
    ([1, 3, 1, 3]),
    ([0, 1, 3, 3]),
    ([0, 0, 1, 3]),
    ([0, 0, 0, 4]),
    ([0, 0, 4, 4]),
    ([1, 3, 4, 4]),
    ([4, 4, 4, 4])
])
def test_get_p_h_needed(dfs, cell_configs):
    threshold_h = 25
    threshold_p = 50
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid,
                                                         cell_configs,
                                                         upper_lower='lower',
                                                         remake_cls_4=True)
    IDS = DF.ID500m.to_list()
    for id in IDS:
        df_pt_to_check = DF_GRID_PT[(DF_GRID_PT.ID500m == id) &
                                    (DF_GRID_PT.p > 0)]
        p_needed, h_needed = gridgran.get_p_h_needed(df_pt_to_check,
                                                     threshold_p, threshold_h)
        if threshold_p - df_pt_to_check.p.sum() <= 0:
            p_needed_expected = 1
        else:
            p_needed_expected = threshold_p - df_pt_to_check.p.sum()
        if threshold_h - len(df_pt_to_check) <= 0:
            h_needed_expected = 1
        else:
            h_needed_expected = threshold_h - len(df_pt_to_check)
        assert p_needed == p_needed_expected
        assert h_needed == h_needed_expected


@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 3]),
    ([1, 3, 3, 3]),
    ([1, 3, 1, 3]),
    ([0, 1, 3, 3]),
    ([0, 0, 1, 3]),
    ([0, 0, 0, 4]),
    ([0, 0, 4, 4]),
    ([1, 3, 4, 4]),
    ([4, 4, 4, 4])
])
def test_get_excess_df(dfs, cell_configs):
    threshold_h = 25
    threshold_p = 50
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid,
                                                         cell_configs,
                                                         upper_lower='lower',
                                                         remake_cls_4=False)
    df_excess_pt, df_3_pt, df_remainder_pt = gridgran.get_excess_df(DF,
                                                                    DF_GRID_PT,
                                                                    DF_GRID,
                                                                    "ID500m",
                                                                    threshold_h=25,
                                                                    threshold_p=50,
                                                                    num_iterations=100,
                                                                    sample_increase_frequency=10,
                                                                    number_to_increase_sample=1,
                                                                    )
    if not df_remainder_pt.empty:
        assert np.all(gridgran.aggregrid(df_remainder_pt, CLASSIFICATION_DICT,
                                         level='ID500m').classification.isin(
            [0, 4]))
    assert len(df_excess_pt) + len(df_3_pt) + len(df_remainder_pt) == len(
        DF_GRID_PT)
    assert df_excess_pt.p.sum() + df_3_pt.p.sum() + df_remainder_pt.p.sum() \
           == \
           DF_GRID_PT.p.sum()

@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 3]),
    ([1, 3, 3, 3]),
    ([1, 3, 1, 3]),
    ([0, 1, 3, 3]),
    ([0, 0, 1, 3]),
    ([0, 0, 0, 4]),
    ([0, 0, 4, 4]),
    ([1, 3, 4, 4]),
    ([4, 4, 4, 4])
])
def test_get_list_of_excess_cls_4_and_remainder_of_cls4(dfs, cell_configs):
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid,
                                                         cell_configs,
                                                         upper_lower='upper',
                                                         )
    excess_list, rest_list = \
        gridgran.get_list_of_execess_cls_4_and_remainder_of_cls4(DF_GRID_PT,
                                                                 DF,
                                                                 "ID500m",
                                                                 )
    assert isinstance(excess_list, list)
    assert isinstance(rest_list, list)
    if 4 in cell_configs:
        df_excess = pd.concat(excess_list)
        df_rest = pd.concat(rest_list)
        df_agg = gridgran.aggregrid(df_rest, CLASSIFICATION_DICT,
                                    level="ID500m")
        assert np.all(df_agg.classification == 4)





@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 3]),
    ([1, 3, 3, 3]),
    ([1, 3, 1, 3]),
    ([0, 1, 3, 3]),
    ([0, 0, 1, 3]),
    ([1, 3, 4, 4]),
    ([1, 1, 3, 4])
])
def test_check_cls_3_can_become_cls_4_PASS(dfs, cell_configs):
    threshold_h = 25
    threshold_p = 50
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid,
                                                         cell_configs,
                                                         upper_lower='upper')
    ok_to_move, df_3, df_excess, df_the_rest = \
        gridgran.check_cls_3_can_become_cls_4(DF, DF_GRID_PT, DF_GRID,
                                              "ID500m", threshold_p=50,
                                              threshold_h=25)
    assert ok_to_move
    assert len(df_3) + len(df_excess) + len(df_the_rest) == len(DF_GRID_PT)
    assert df_3.p.sum() + df_excess.p.sum() + df_the_rest.p.sum() == \
           DF_GRID_PT.p.sum()


@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 3]),
    ([1, 3, 3, 3]),
    ([1, 3, 1, 3]),
    ([0, 1, 3, 3]),
    ([0, 0, 1, 3]),
    ([1, 3, 4, 4]),
    ([1, 1, 3, 4])
])
def test_check_cls_3_can_become_cls_4_FAIL(dfs, cell_configs):
    threshold_h = 25
    threshold_p = 50
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid,
                                                         cell_configs,
                                                         upper_lower='lower',
                                                         remake_cls_4=True)
    ok_to_move, df_3, df_excess, df_the_rest = \
        gridgran.check_cls_3_can_become_cls_4(DF, DF_GRID_PT, DF_GRID,
                                              "ID500m", threshold_p=50,
                                              threshold_h=25)
    assert not ok_to_move
    assert df_3.empty
    assert df_excess.empty
    assert df_the_rest.empty

@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 3]),
    ([1, 3, 3, 3]),
    ([1, 3, 1, 3]),
    ([0, 1, 3, 3]),
    ([0, 0, 1, 3]),
    ([1, 3, 4, 4]),
    ([1, 1, 3, 4])
])
def test_make_cls_3_to_cls_4_PASS(dfs, cell_configs):
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid,
                                                         cell_configs,
                                                         upper_lower='upper')
    ok_to_move, df_3_pt, df_excess_pt, df_remainder_pt = \
        gridgran.check_cls_3_can_become_cls_4(DF,
                                              DF_GRID_PT,
                                              DF_GRID,
                                              "ID500m"
                                              )
    try:
        df_grid_moved, df_grid_pt_moved = gridgran.make_cls_3_to_cls_4(
            df_3_pt,
            df_excess_pt,
            df_remainder_pt,
            "ID500m",
            CLASSIFICATION_DICT
        )
        df_agg = gridgran.aggregrid(df_grid_pt_moved, CLASSIFICATION_DICT,
                                    level='ID500m')
        assert list(df_agg.classification.unique()) == [0, 4]
        assert df_grid_pt_moved.p.sum() == DF_GRID_PT.p.sum()
    #This exception will be tested in another test. Some cell configs will
    # fail randomly due to insufficient points being available
    except gridgran.DataFrameNotOverDisclosureLimitException as e:
        assert True

@pytest.mark.parametrize("cell_configs", [
    ([1, 1, 1, 3]),
    ([1, 3, 3, 3]),
    ([1, 3, 1, 3]),
    ([0, 1, 3, 3]),
    ([0, 0, 1, 3]),
    ([1, 3, 4, 4]),
    ([1, 1, 3, 4])
])
def test_make_cls_3_to_cls_4_FAIL(dfs, cell_configs):
    df_grid, df_grid_pt, df = dfs
    DF, DF_GRID_PT, DF_GRID = tests.make_any_combination(df, df_grid_pt,
                                                         df_grid,
                                                         cell_configs,
                                                         upper_lower='upper',
                                                         remake_cls_4=False)
    ok_to_move, df_3_pt, df_excess_pt, df_remainder_pt = \
        gridgran.check_cls_3_can_become_cls_4(DF,
                                              DF_GRID_PT,
                                              DF_GRID,
                                              "ID500m"
                                           )
    # Set up dataframes for failure
    df_3_pt.p.values[:] = 0
    df_3_pt.p.values[:] = 0
    df_excess_pt.p.values[:] = 0
    df_excess_pt.p.values[:] = 0
    df_remainder_pt.p.values[:] = 0
    df_remainder_pt.p.values[:] = 0

    with pytest.raises(gridgran.DataFrameNotOverDisclosureLimitException) as\
            exec:
        df_grid_moved, df_grid_pt_moved = gridgran.make_cls_3_to_cls_4(
            df_3_pt,
            df_excess_pt,
            df_remainder_pt,
            "ID500m",
            CLASSIFICATION_DICT,
        )

