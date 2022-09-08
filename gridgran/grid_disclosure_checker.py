"""Module with class to tie together all functions in a class to handle grid \
 disaggregation at a 1km cell level
"""
import pandas as pd

import gridgran


class GridDisclosureChecker:
    """Class iterates through each level of 1km grid cell down to 125m to
    check whether each child cell is above disclosure limit. """

    def __init__(self,
                 df,
                 df_grid,
                 df_grid_pt,
                 classification_settings,
                 threshold_p=50,
                 threshold_h=25,
                 cls_2_prp=0,
                 num_iterations=100,
                 sample_increase_frequency=10,
                 number_to_increase_sample=1):
        """
        Class instantiation

        Parameters:
        ------------
        df : (pd.DataFrame)
            Input points aggregated to 500m level

        df_grid : (pd.DataFrame)
            Points aggregated and classified to 125m level

        df_grid_pt : (pd.DataFrame)
            All points joined with 125m cells (empty cells also joined)

        classification_settings : (dict)
            Dictionary with keys/values for household thresholds with following
            keys/values as example (values can be changed but keys should
            remain the same):
            {
            'p_1': 10,
            'p_2': 40,
            'p_3': 50,
            'h_1': 5,
            'h_2': 20,
            'h_3': 25,
            }
            Also contains boleans indicating levels at which to ignore cls2
            thresholds:


        threshold_p : (int)
            Threshold for disclosure limit of population (DEFAULT=50)

        threshold_h : (int)
            Threshold for disclosure limit of number of housedholds \
            (DEFAULT = 25)

        cls_2_prp : (float)
            Cells classified as Cls2 and their neighbours within the same
            parent will be aggregated up to parent level class 2
            pop/households are above cls_2_prp proportion relative to total
            pop/households in all neighbours within parent cell. Proportion
            should be given (between 0 and 1) and NOT percentage \
            (i.e. 0.1 = 10%)
            DEFAULT=0

        num_iterations : (int)
            Number to times to try to bring classes below disclosure limit
            to class 4 (above limit) (DEFAULT = 100)

        sample_increase_frequency : (int)
            How often (num_iterations) to increase the number of rows by
            which the dataframe is sampled in attempt to bring cell over
            disclosure limit (DEFAULT = 10)

        number_to_increase_sample : (int)
            Number by which to increase the sample size every
            sample_increase_frequency iterations

        """
        self.df = df
        self.df_grid = df_grid
        self.df_grid_pt = df_grid_pt
        self.classification_settings = classification_settings
        self.classification_dict = \
            self.classification_settings["classification_dict"]
        self.threshold_p = threshold_p
        self.threshold_h = threshold_h
        self.cls_2_prp = cls_2_prp
        self.num_iterations = num_iterations
        self.sample_increase_frequency = sample_increase_frequency
        self.number_to_increase_sample = number_to_increase_sample
        self.global_grid_list = []  # list to hold grid dataframes as they are
        # processed
        self.global_grid_pt_list = []  # list to hold point dataframes as they
        # are processed

    def execute(self):
        """
        Iterates through functions in each level until children cells
        cannot be classified over disclosure limit

        Parameters:
        -----------
        None

        Returns:
        --------
        global_grid : (pd.DataFrame)
            Dataframe of processed/classified grid cells with dissolve_id
            field completed for all rows

        global_grid_pt : (pd.DataFrame)
            Dataframe of points in cells with record of where cells have
            moved from
        """
        # ID500m LEVEL
        class_dict = self.classification_dict.copy()
        if self.classification_settings["cls_2_threshold_500m"]:
            class_dict['p_2'] = None
            class_dict['h_2'] = None
        df_grid_500, df_grid_pt_500, df_500, child_cells_valid_500 = \
            gridgran.check_cells_children_are_valid(self.df,
                                                    self.df_grid,
                                                    self.df_grid_pt,
                                                    "ID500m",
                                                    "ID1000m",
                                                    "ID250m",
                                                    class_dict,
                                                    cls_2_prp=self.cls_2_prp)
        if child_cells_valid_500:
            class_dict = self.classification_dict.copy()
            if self.classification_settings["cls_2_threshold_250m"]:
                class_dict['p_2'] = None
                class_dict['h_2'] = None
            for id_250 in gridgran.get_children_ids(df_500, "ID500m"):
                df_grid_500_subset, df_grid_pt_500_subset, df_500_subset = \
                    gridgran.subset_by_id(df_grid_500, df_grid_pt_500,
                                          "ID500m", "ID250m", id_250,
                                          class_dict,
                                          cls_2_prp=self.cls_2_prp, )
                df_grid_250, df_grid_pt_250, df_250, child_cells_valid_250 = \
                    gridgran.check_cells_children_are_valid(
                        df_500_subset,
                        df_grid_500_subset,
                        df_grid_pt_500_subset,
                        "ID250m",
                        "ID500m",
                        "ID125m",
                        class_dict,
                        cls_2_prp=self.cls_2_prp)
                if child_cells_valid_250:
                    class_dict = self.classification_dict.copy()
                    if self.classification_settings["cls_2_threshold_125m"]:
                        class_dict['p_2'] = None
                        class_dict['h_2'] = None
                    for id_125 in gridgran.get_children_ids(df_250, "ID250m"):
                        df_grid_250_subset, df_grid_pt_250_subset, \
                            df_250_subset = gridgran.subset_by_id(
                                df_grid_250,
                                df_grid_pt_250,
                                "ID250m",
                                "ID125m", id_125,
                                class_dict,
                                cls_2_prp=self.cls_2_prp)
                        df_grid_125, df_grid_pt_125, df_125, \
                            child_cells_valid_125 = \
                            gridgran.check_cells_children_are_valid(
                                df_250_subset,
                                df_grid_250_subset,
                                df_grid_pt_250_subset,
                                "ID125m",
                                "ID250m",
                                "ID125m",
                                class_dict,
                                cls_2_prp=self.cls_2_prp)
                        if child_cells_valid_125:
                            df_grid_125 = df_grid_125.copy()
                            df_grid_125.loc[:, 'dissolve_id'] = \
                                df_grid_125.copy().ID125m
                            self.global_grid_list.append(df_grid_125)
                            self.global_grid_pt_list.append(df_grid_pt_125)

                        else:
                            self.global_grid_list.append(df_grid_125)
                            self.global_grid_pt_list.append(df_grid_pt_125)
                else:
                    self.global_grid_list.append(df_grid_250)
                    self.global_grid_pt_list.append(df_grid_pt_250)
        else:
            self.global_grid_list.append(df_grid_500)
            self.global_grid_pt_list.append(df_grid_pt_500)
        grid_final = pd.concat(self.global_grid_list)
        point_final = pd.concat(self.global_grid_pt_list)
        return grid_final, point_final
