"""Utilities to help moving points between cells"""
import pandas as pd
import random

import gridgran


def change_to_random_id(row, ids_to_change_to):
    """
    Helper function that changes the row's ID's based on a random selection
    from the dictionary in ids_to_change_to

    Parameters:
    -----------
    row : (pd.Series)
        Row to change IDs

    ids_to_change_to : (list)
        List of dictionaries with each holding the all the levels' IDS
        corresponding to the row identified in id_list and level
    """
    id_dict = random.choice(ids_to_change_to)
    row.ID125m = id_dict["ID125m"]
    row.ID250m = id_dict["ID250m"]
    row.ID500m = id_dict["ID500m"]
    row.ID500m = id_dict["ID500m"]
    return row


def get_excess_df(df,
                  df_grid_pt,
                  df_grid,
                  current_level,
                  threshold_h=25,
                  threshold_p=50,
                  num_iterations=100,
                  sample_increase_frequency=10,
                  number_to_increase_sample=1, ):
    """Returns dataframe of 'excess points' gather from class 1 cells in DF,
     as well as overflow points in class 4. Also returns dataframe of class 3
      points, as well as all remaining rows (0 cells and 4 cells not over
       limit)

    Parameters:
    ------------
    df : (pd.DataFrame):
        DataFrame at current level showing classification of cells

    df_grid_pt : (pd.DataFrame):
        All points and empty grid cells that will be separated out

    df_grid : (pd.DatFrame):
        Dataframe of points aggregated to 125m level

    current_level : (str)
        Level at which analysis is being carried out from ["ID125m",
        "ID250m", "ID500m", "ID1000m"]

    threshold_h : (int)
        Number of households to meet disclosure limit

    threshold_p : (int)
        Pop sum to meet disclosure limit

    num_iterations : (int)
        Algorithm is run in a while loop. To prevent infinite loops in
        finding optimum threshold, the while loop will break after
        num_iterations. (DEFAULT = 100) - FOR USE IN
        separate_excess_rows_in_df_cls_4()

    sample_increase_frequency : (int)
        Sample n (threshold_h) increases by number_to_increase_sample every
        sample_increase_frequency iterations in while loop. (DEFAULT = 5) -
         FOR USE IN
        separate_excess_rows_in_df_cls_4()

    number_to_increase_sample : (int)
        Number by which to increase the sample rows number every
        sample_increase_frequency (DEFAULT = 5) - FOR USE IN
        separate_excess_rows_in_df_cls_4()


    Returns:
    --------
    df_excess_pt : (pd.DataFrame)
        Excess points that can be moved

    df_3_pt : (pd.DataFrame)
        All points in class 3 at current level

    df_remainder_pt : (pd.DataFrame)
        All remaining point/rows that are not excess or class 3

    """
    df_3_pt = df_grid_pt[df_grid_pt[current_level].isin(
        df[current_level][df.classification == 3].to_list())]
    df_1_pt = df_grid_pt[df_grid_pt[current_level].isin(
        df[current_level][df.classification == 1].to_list())]
    excess_df_list, remainder_df_list = \
        get_list_of_execess_cls_4_and_remainder_of_cls4(
            df_grid_pt,
            df,
            current_level,
            threshold_h=threshold_h,
            threshold_p=threshold_p,
            num_iterations=100,
            sample_increase_frequency=10,
            number_to_increase_sample=1,
        )
    excess_df_list.append(df_1_pt)
    df_everything_remaining = df_grid_pt[df_grid_pt[current_level].isin(
        df[current_level][df.classification.isin([0])])]  # Catch
    # everything else
    remainder_df_list.append(df_everything_remaining)
    excess_points_df = pd.concat(excess_df_list)
    remainder_points_df = pd.concat(remainder_df_list)
    if remainder_points_df.empty:
        remainder_points_df = df_grid_pt.copy()[0:0]
    return excess_points_df, df_3_pt, remainder_points_df


def get_list_of_execess_cls_4_and_remainder_of_cls4(
        df_grid_pt,
        df,
        current_level,
        threshold_h=25,
        threshold_p=50,
        num_iterations=100,
        sample_increase_frequency=10,
        number_to_increase_sample=1,
        ):
    """Returns a list of excess cls 4's in each cell of df, as well as list
    of remainder class 4's that are not excess

    Parameters:
    -----------
    df : (pd.DataFrame):
        DataFrame at current level showing classification of cells

    df_grid_pt : (pd.DataFrame):
        All points and empty grid cells that will be separated out

    current_level : (str)
        Level at which analysis is being carried out from ["ID125m",
        "ID250m", "ID500m", "ID1000m"]

    threshold_h : (int)
        Number of households to meet disclosure limit

    threshold_p : (int)
        Pop sum to meet disclosure limit

    num_iterations : (int)
        Algorithm is run in a while loop. To prevent infinite loops in
        finding optimum threshold, the while loop will break after
        num_iterations. (DEFAULT = 100) - FOR USE IN
        separate_excess_rows_in_df_cls_4()

    sample_increase_frequency : (int)
        Sample n (threshold_h) increases by number_to_increase_sample every
        sample_increase_frequency iterations in while loop. (DEFAULT = 5) -
         FOR USE IN
        separate_excess_rows_in_df_cls_4()

    number_to_increase_sample : (int)
        Number by which to increase the sample rows number every
        sample_increase_frequency (DEFAULT = 5) - FOR USE IN
        separate_excess_rows_in_df_cls_4()

    Returns:
    ---------
    excess_df_list : (list)
        List of dataframes from each cell id in df where that are excess
        points above the threshold - leaving enough points behind to keep
        cell above threshold

    remainder_df_list : (list)
        All the rest of dataframes in cells that are class 4
    """
    excess_df_list = []
    remainder_df_list = []
    df_4_pt = df_grid_pt[df_grid_pt[current_level].isin(
        df[current_level][df.classification == 4].to_list())]
    if not df_4_pt.empty:
        IDS_4 = list(df_4_pt[current_level].unique())
        for id in IDS_4:
            df_4 = df_4_pt[df_4_pt[current_level] == id]
            try:
                df_pt_separated, df_pt_excess = \
                    separate_excess_rows_in_df_cls_4(
                        df_4,
                        threshold_h=threshold_h,
                        threshold_p=threshold_p,
                        num_iterations=num_iterations,
                        sample_increase_frequency=sample_increase_frequency,
                        number_to_increase_sample=number_to_increase_sample,
                        )
                # if not df_pt_excess.empty:
                excess_df_list.append(df_pt_excess)
                remainder_df_list.append(df_pt_separated)
            except gridgran.DataFrameCouldNotBeSeparatedException:
                remainder_df_list.append(df_4)  # If nothing can be separated
                # out, append all the rows to excess list
    return excess_df_list, remainder_df_list


def separate_excess_rows_in_df_cls_4(df_pt,
                                     threshold_h=25,
                                     threshold_p=50,
                                     num_iterations=100,
                                     sample_increase_frequency=10,
                                     number_to_increase_sample=1,
                                     ):
    """
    Function randomly samples df_pt by threshold_h rows iteratively until
    df_pt['p'].sum() is within perc_closeness percent (must be equal or
    above) of threshold_p. If this stipulation is met, the subset/sample
    will be returned along with the remaining points in a separate dataframe.

    Algorithm will only be run num_iterations until threshold is met. If
    after sample_increase_frequency the threshold isn't met, threshold_h
    will be increased by number_to_increase_sample to try to get the correct
     value.

    Parameters:
    -----------
    df_pt : (pd.DataFrame)
        Dataframe representing points/grids to be separated into dataframe
        meeting threshold as well as remainder points

    threshold_h : (int)
        Value of rows (number of households/points) to randomly sample in
        each iteration. (DEFAULT = 25)

    threshold_p : (int)
        Value of population sum used to define when threshold is met for
        dataframe (DEFAULT = 50)

    num_iterations : (int)
        Algorithm is run in a while loop. To prevent infinite loops in
        finding optimum threshold, the while loop will break after
        num_iterations. (DEFAULT = 100)

    sample_increase_frequency : (int)
        Sample n (threshold_h) increases by number_to_increase_sample every
        sample_increase_frequency iterations in while loop. (DEFAULT = 5)

    number_to_increase_sample : (int)
        Number by which to increase the sample rows number every
        sample_increase_frequency (DEFAULT = 5)
     """
    counter = 0  # Count number of iterations
    optimal_reached = False  # Switch to break while loop
    best_match = 1000
    best_match_df = None
    while not optimal_reached:
        if (len(df_pt[df_pt.p > 0]) - threshold_h >= threshold_h) & \
                (df_pt[df_pt.p > 0].p.sum() - threshold_p >= threshold_p):
            sample_df = df_pt[df_pt.p > 0].sample(
                n=threshold_h)  # subset a sample
            pop = sample_df.p.sum()
            if pop - threshold_p >= 0:  # Pop should be more than the theshold
                if pop - threshold_p < best_match:
                    best_match = pop - threshold_p
                    best_match_df = sample_df.copy()
                # if sample_df.p.mean() <= df_pt.p.mean():
                if (sample_df.p.mean() - df_pt.p.mean()) / df_pt.p.mean() \
                        <= 0.05:
                    # Is the sample mean within 5% of the whole population
                    # mean?
                    optimal_reached = True
            counter += 1
            if counter % sample_increase_frequency == 0:
                threshold_h += number_to_increase_sample
            if counter >= num_iterations:
                optimal_reached = True
        else:
            optimal_reached = True
    df_pt_separated, df_pt_excess = get_separated_points_and_excess_points(
        best_match_df, df_pt, threshold_p, threshold_h)
    return df_pt_separated, df_pt_excess


def get_separated_points_and_excess_points(best_match_df,
                                           df_pt,
                                           threshold_p,
                                           threshold_h):
    """Checks best match df points are valid and separates them from df_pt
    if they are. Raises exception if not

    Parameters:
    -----------
    best_match_df : (pd.DataFrame)
        Dataframe separated from df_pt to check if valid for use

    df_pt : (pd.DataFrame)
        Point dataframe that is attempting to be separated into excess
        points

    threshold_p : (int)
        Threshold population

    threshold_h : (int)
        Threshold number of households

    Returns:
    ---------
    df_pt_separated : (pd.Dataframe)
        Points separated from df_pt

    df_pt_excesss : (pd.DataFrame)
        Points above threhold
    """
    try:
        assert isinstance(best_match_df, pd.DataFrame)
        df_pt_separated = df_pt[
            df_pt.ID125m.isin(best_match_df.ID125m.unique())]
        assert df_pt_separated.p.sum() >= threshold_p
        assert len(df_pt_separated[df_pt_separated.p > 0]) >= threshold_h
        df_pt_excess = df_pt[~df_pt.ID125m.isin(best_match_df.ID125m.unique())]
    except AssertionError:
        raise gridgran.DataFrameCouldNotBeSeparatedException(
            "DataFrame could not be separated effectively to meet threshold")
    return df_pt_separated, df_pt_excess


def move_cls_1_to_4(df, df_grid, df_grid_pt, current_level, parent_level,
                    child_level, classification_dict, cls_2_prp=0):
    """
    Moves all points (from df_grid_pt) in cells with class 1 in df to
    cells in df with class 4. df_grid_pt is then reaggregated to df_grid
    with new values, which is then reaggregated to df to reflect cells that
    were previously class 1 to class 0

    Parameters:
    -----------
    df : (pd.DataFrame)
        DataFrame aggregated to current level with classifications in assigned

    df_grid : (pd.DataFrame)
        Grid dataframe aggregated to 125m level

    df_grid_pt : (pd.DataFrame)
        Grids joined to points

    current_level : (str)
        Current resolution column in df being processed (i.e. 500m, 100m etc)

    parent_level :  (str)
        Parent level of current level being processed

    child_level :   (str)
        Child level of current level being processed

    classification_dict : (dict)
        Dictionary with keys/values for household thresholds with following
        keys/values as example (values can be changed but keys should remain \
         the same):
        {
        'p_1': 10,
        'p_2': 40,
        'p_3': 50,
        'h_1': 5,
        'h_2': 20,
        'h_3': 25,
        }

    cls_2_prp : (float)
        Cells classified as Cls2 and their neighbours within the same
        parent will be aggregated up to parent level class 2
        pop/households are above cls_2_prp proportion relative to total
        pop/households in all neighbours within parent cell. Proportion
        should be given (between 0 and 1) and NOT percentage (i.e. 0.1 = 10%)
        DEFAULT=0

    Returns:
    --------
    df_grid : (pd.DataFrame)
        Processed input grid with ID125m ID's assigned accordingly based on \
         what level data will be disaggregated (aggregated up or kept as is).

    df_grid_pt : (pd.DataFrame)
        Processed input df_grid_pt with rows removed where data is
        aggregated up.
    """
    ids_1 = gridgran.get_ids(df, current_level, 1)
    ids_4 = gridgran.get_ids(df, current_level, 4)
    ids_to_change_to = gridgran.get_list_of_rowIDS_for_list_of_IDS(
        df_grid, ids_4, current_level)
    points_to_move = df_grid_pt[df_grid_pt[current_level].isin(ids_1)]
    df_grid_pt = df_grid_pt[~df_grid_pt[current_level].isin(ids_1)]
    rows_to_insert_back = points_to_move.copy()
    rows_to_insert_back.loc[:, 'p'] = 0
    rows_to_insert_back.loc[:, 'h'] = 0
    col = f'{current_level}_LEVEL_MOVE_ORIGIN'
    # if len(points_to_move):
    points_to_move = points_to_move.reset_index(drop=True)
    points_to_move.loc[:, col] = points_to_move.ID125m.copy()
    points_to_move = points_to_move.apply(change_to_random_id,
                                          ids_to_change_to=ids_to_change_to,
                                          axis=1).copy()
    ROWS_TO_REPLACE = pd.concat([points_to_move,
                                 rows_to_insert_back]).reset_index(drop=True)
    df_grid_pt = pd.concat([df_grid_pt, ROWS_TO_REPLACE])
    df_grid = gridgran.aggregrid(df_grid_pt, classification_dict,
                                 level='ID125m', template=True,
                                 cls_2_prp=cls_2_prp)
    return df_grid, df_grid_pt


def make_cls_3_to_cls_4(df_3_pt,
                        df_excess_pt,
                        df_remainder_pt,
                        current_level,
                        classification_dict,
                        threshold_h=25,
                        threshold_p=50,
                        num_iterations=100,
                        sample_increase_frequency=10,
                        number_to_increase_sample=1,
                        cls_2_prp=0
                        ):
    """Function makes attempt at bringing class 3 df over threshold using
    excess points. All dataFrames are then concatenated and returned. If any
     df_3 can not be brought over disclosure threshold, all dataframes are
      concatenated and aggregated up to parent level

    Parameters:
    ------------

    df_3pt : (pd.DataFrame)
        Dataframe of points in class 3 cells

    df_excess_pt : (pd.DataFrame)
        Dataframe of all excess points that can be moved

    df_remainder_pt : (df.DataFrame)
        All other class 4 points and class 0 empty cells

    current_level : (str)
        Current id level

    classification_dict : (dict)
        Dictionary with keys/values for household thresholds with following
        keys/values as example (values can be changed but keys should remain
         the same):
        {
        'p_1': 10,
        'p_2': 40,
        'p_3': 50,
        'h_1': 5,
        'h_2': 20,
        'h_3': 25,
        }

    threshold_p : (int)
        Pop sum required to bring cell over disclosure limit

    threshold_h : (int)
        Num households to bring cell over disclosure limit

     num_iterations : (int)
        Algorithm is run in a while loop. To prevent infinite loops in
        finding optimum threshold, the while loop will break after
        num_iterations. (DEFAULT = 100)

    sample_increase_frequency : (int)
        Sample n (threshold_h) increases by number_to_increase_sample every
        sample_increase_frequency iterations in while loop. (DEFAULT = 10)

    number_to_increase_sample : (int)
        Number by which to increase the sample rows number every
        sample_increase_frequency (DEFAULT = 1)

    cls_2_prp : (float)
        Cells classified as Cls2 and their neighbours within the same
        parent will be aggregated up to parent level class 2
        pop/households are above cls_2_prp proportion relative to total
        pop/households in all neighbours within parent cell. Proportion
        should be given (between 0 and 1) and NOT percentage (i.e. 0.1 = 10%)
        DEFAULT=0


    Raises:
    ------
    DataFrameNotOverDisclosureLimitException
        When all points cannot be moved into class 3 to bring them over to
        class 4
    """
    df_3_to_4_list = []
    for index, id_3 in enumerate(df_3_pt[current_level].unique()):
        df_3 = df_3_pt[df_3_pt[current_level] == id_3]
        p_needed, h_needed = get_p_h_needed(df_3, threshold_p, threshold_h)
        if index == len(list(df_3_pt[current_level].unique())) - 1:  # If this
            # is the last ID in df_3_pt, pass ALL excess points to it
            if (len(df_excess_pt) >= h_needed) & (df_excess_pt.p.sum() >=
                                                  p_needed):
                best_match_df = df_excess_pt[df_excess_pt.p > 0].copy()
            else:
                best_match_df = None
        else:  # Else only take what's needed from excess points
            best_match_df = get_excess_points_for_cls_3_df(
                df_excess_pt,
                p_needed,
                h_needed,
                num_iterations=100,
                sample_increase_frequency=10,
                number_to_increase_sample=1)
        try:
            assert isinstance(best_match_df, pd.DataFrame)
            df_grid_tmp = gridgran.aggregrid(df_3, classification_dict,
                                             level='ID125m',
                                             cls_2_prp=cls_2_prp)
            ids_to_change_to = gridgran.get_list_of_rowIDS_for_list_of_IDS(
                df_grid_tmp, [id_3], current_level)
            df_excess_pt.loc[best_match_df.index, 'p'] = 0
            df_excess_pt.loc[best_match_df.index, 'h'] = 0
            col = f'{current_level}_LEVEL_MOVE_ORIGIN'
            best_match_df = best_match_df.reset_index(drop=True)
            best_match_df.loc[:, col] = best_match_df.ID125m.copy()
            best_match_df = best_match_df.apply(
                change_to_random_id,
                ids_to_change_to=ids_to_change_to,
                axis=1).copy()
            df_3_to_4_list.append(pd.concat([df_3, best_match_df]))
        except AssertionError:
            raise gridgran.DataFrameNotOverDisclosureLimitException

    df_3_to_4 = pd.concat(df_3_to_4_list)
    df_grid_pt = pd.concat([df_3_to_4, df_excess_pt, df_remainder_pt])

    df_grid = gridgran.aggregrid(df_grid_pt, classification_dict,
                                 level="ID125m", template=True,
                                 cls_2_prp=cls_2_prp)
    return df_grid, df_grid_pt


def get_excess_points_for_cls_3_df(df_excess_pt,
                                   p_needed,
                                   h_needed,
                                   num_iterations=100,
                                   sample_increase_frequency=10,
                                   number_to_increase_sample=1
                                   ):
    """
    Returns best match df to match p and h needed as extracted from
    df_excess_pt

    Parameters:
    -----------
    df_excess_pt : (pd.DataFrame)
        Dataframe of points from which cls 3 df and take to make it class 4

    p_needed : (int)
        Population needed to take cls 3 to cls 4

    h_needed : (int)
        Households needed to take cls 3 to cls 4

    num_iterations : (int)
        Algorithm is run in a while loop. To prevent infinite loops in
        finding optimum threshold, the while loop will break after
        num_iterations. (DEFAULT = 100)

    sample_increase_frequency : (int)
        Sample n (threshold_h) increases by number_to_increase_sample every
        sample_increase_frequency iterations in while loop. (DEFAULT = 10)

    number_to_increase_sample : (int)
        Number by which to increase the sample rows number every
        sample_increase_frequency (DEFAULT = 1)


    Raises:
    ------
    DataFrameNotOverDisclosureLimitException
        When all points cannot be moved into class 3 to bring them over to
        class 4

    Returns:
    --------
    best_match_df : (pd.DataFrame)
        Function tries to match p and h needed as closely as possible and
        returns those points extraced. Else raises
         DataFrameNotOverDisclosureLimitException

    """
    counter = 0  # Count number of iterations
    optimal_reached = False  # Switch to break while loop
    best_match = 1000
    best_match_df = None
    while not optimal_reached:
        if len(df_excess_pt[df_excess_pt.p > 0]) >= h_needed:
            df_sample = df_excess_pt[df_excess_pt.p > 0].sample(n=h_needed)
        else:
            raise gridgran.DataFrameNotOverDisclosureLimitException
        pop = df_sample.p.sum()
        if pop >= p_needed:
            if pop - p_needed < best_match:
                best_match = pop - pop - p_needed
                best_match_df = df_sample.copy()
            if (pop - p_needed) / p_needed <= 0.1:  # Within 5%
                optimal_reached = True
                break
        counter += 1
        if counter % sample_increase_frequency == 0:
            h_needed += number_to_increase_sample
        if counter >= num_iterations:
            optimal_reached = True
    return best_match_df


def get_p_h_needed(df_grid_pt, threshold_p,
                   threshold_h):
    """Returns population and households needed for df_grid_pt to meet
    threshold_p and threshold_h

    Parameters:
    -----------
    df_grid_pt : (pd.DataFrame)
        Dataframe to check count of households and sum of population.

    threshold_p : (int)
        Threshold needed to go over disclosure limit for pop sum

    threshold_h : (int)
        Threshold needed to go over disclosure limit for num households
    """
    p = df_grid_pt.p.sum()
    h = len(df_grid_pt)
    p_needed = threshold_p - p
    h_needed = threshold_h - h
    if p_needed <= 0:
        p_needed = 1
    if h_needed <= 0:
        h_needed = 1
    return p_needed, h_needed


def check_cls_3_can_become_cls_4(df,
                                 df_grid_pt,
                                 df_grid,
                                 current_level,
                                 threshold_p=50,
                                 threshold_h=25,
                                 num_iterations=100,
                                 sample_increase_frequency=10,
                                 number_to_increase_sample=1
                                 ):
    """Checks to see if there are enough excess points in df_grid_pt to
    bring class 3 cells over disclosure limit. If returns True, df_3_pt,
    df_excess and df_remainder points will be returned, else they will be
    returned as None

    Parameters:
    -----------
    df : (pd.DataFrame)
        Dataframe aggregated to current level (4 cells)

    df_grid_pt : (pd.DataFrame)
        Dataframe of all points/empty cells in current level len == n points
         + n empty cells

    df_grid : (df.DataFrame)
        Global dataframe of all cells in current level down to all children
        cells

    threshold_p : (int)
        Pop sum required to bring cell over disclosure limit

    threshold_h : (int)
        Num households to bring cell over disclosure limit

     num_iterations : (int)
        Algorithm is run in a while loop. To prevent infinite loops in
        finding optimum threshold, the while loop will break after
        num_iterations. (DEFAULT = 100)

    sample_increase_frequency : (int)
        Sample n (threshold_h) increases by number_to_increase_sample every
        sample_increase_frequency iterations in while loop. (DEFAULT = 5)

    number_to_increase_sample : (int)
        Number by which to increase the sample rows number every
        sample_increase_frequency (DEFAULT = 5)

    Returns:
    ---------
    ok_to_move : (bool)
        True if enough cells to fill all class 3 cells to make them class 4,
         else False

    df_3_pt : (pd.DataFrame)
        DataFrame of all points in class 3 cells

    df_excess_pt : (pd.DataFrame)
        DataFrame of all extra points gathered from class 1 cells as well as
         excess points from class 4 cells that can be used to fill class 3
          cells.

    remainder_points_df : (pd.DataFrame)
        All remaining points (not excess or in class 1/4)
    """
    df_excess_pt, df_3_pt, df_remainder_pt = gridgran.get_excess_df(
        df,
        df_grid_pt,
        df_grid,
        current_level,
        threshold_h=threshold_h,
        threshold_p=threshold_p,
        num_iterations=num_iterations,
        sample_increase_frequency=sample_increase_frequency,
        number_to_increase_sample=number_to_increase_sample
        )
    ok_to_move = False
    IDS = df_3_pt[current_level].unique()
    p_needed_total = 0
    h_needed_total = 0
    if len(IDS):
        for id in IDS:
            df_3 = df_3_pt[(df_3_pt[current_level] == id) & (df_3_pt.p > 0)]
            p_needed, h_needed = get_p_h_needed(df_3, threshold_p,
                                                threshold_h)
            p_needed_total += p_needed
            h_needed_total += h_needed

    p_available = df_excess_pt.p.sum()
    h_available = len(df_excess_pt)
    if (p_needed_total <= p_available) & (h_needed_total <= h_available):
        ok_to_move = True
    if not ok_to_move:
        df_3_pt = df_grid_pt.copy()[0:0]
        df_excess_pt = df_grid_pt.copy()[0:0]
        df_remainder_pt = df_grid_pt.copy()[0:0]
    return ok_to_move, df_3_pt, df_excess_pt, df_remainder_pt
