"""Module with class to process grids within 1km GRID cell at a time for use
when using multiprocessing over large areas """

import geopandas as gpd
import pandas as pd

import gridgran


class GridGranulatorSingleCell:
    """Takes inputs for single 1km cell and saves layer to geopacakage"""

    def __init__(
            self,
            gdf_1km,
            gdf_125m,
            gdf_pts,
            gdf_water_clip,
            outpath,
            outlayer,
            outcsv,
            classification_settings,
            class_2_threshold_prp=0.05,
            fill_values_below_threshold_with='minimum'
    ):
        """Initialisation

        Parameters:
        -----------
        gdf_1km : gpd.GeoDataFrame
            1km cell

        gdf_125m : gpd.GeoDataFrame
            125m cells within 1km

        gdf_pts : gpd.GeoDataFrame
            Points within 1km

        gdf_water_clip: gpd.GeoDataFrame
            Water clip used for water mask

        outpath : str/Path
            Path to output geopackage

        outlayer : str
            Layer in geopackage in which to save processed grid

        outcsv : str/Path
            Path to output csv

        classification_settings : dict
            Classification settings

        class_2_threshold_prp : float
            If grid cells classified as class 2 are below this proportion of \
             the total population within neighbouring cells within parent
             cell, they will be reclassified as class 1 - else all cells
             will  be aggregated up. This is to stop class 2 cells causing
             densely populated cells within parent cells being aggregated up \
              to parent level. DEFAULT 0.05 (5%)

        fill_values_below_threshold_with : str
            Options ['minimum', 'star', 'null'] -
            In the output grid, cells that finish below the minimum threshold
            will need to be filled with a dummy value so as no to identify
            individuals. The options are 'minimum' (the minimum threshold
            value),
            'star' (asterisk '*') or 'null'/NA. (Default='minimum')
        """
        self.gdf_1km = gdf_1km
        self.gdf_125m = gdf_125m.to_crs(27700)
        self.gdf_pts = gdf_pts
        self.gdf_water_clip = gdf_water_clip
        self.outpath = outpath
        self.outlayer = outlayer
        self.outcsv = outcsv
        self.classification_settings = classification_settings
        self.classification_dict = self.classification_settings[
            'classification_dict']
        self.class_2_threshold_prp = class_2_threshold_prp
        self.fill_values_below_threshold_with = \
            fill_values_below_threshold_with

    def iterate_and_process(self):
        """Process children of 1km cell

        Parameters:
            None

        Returns:
            None
        """
        pops = 0
        GLOBAL_GRID_LIST = []
        GLOBAL_POINT_LIST = []
        df_grid_125, df_grid_pt = \
            gridgran.prep_points_and_grid_from_dataframes(
                self.gdf_125m,
                self.gdf_pts,
                self.classification_dict,
                self.class_2_threshold_prp)
        # for row in self.gdf_1km.itertuples():
        row = self.gdf_1km
        cell_125 = self.gdf_125m[
            self.gdf_125m.GridID125m.str.startswith(row.GridID1km[:-3])]
        df_grid_in_cell = df_grid_125[df_grid_125.ID1000m == row.GridID1km]
        df_grid_pt_in_cell = df_grid_pt[df_grid_pt.ID1000m ==
                                        row.GridID1km]
        if df_grid_pt_in_cell.p.sum() > 0:
            pop_in = df_grid_pt_in_cell.p.sum()
            pops += pop_in
            df = gridgran.aggregrid(df_grid_pt_in_cell,
                                    self.classification_dict,
                                    level='ID500m',
                                    template=False,
                                    cls_2_prp=self.class_2_threshold_prp)
            gran = gridgran.GridDisclosureChecker(
                df,
                df_grid_in_cell,
                df_grid_pt_in_cell,
                self.classification_settings,
                threshold_p=self.classification_dict['p_3'] + 1,
                threshold_h=self.classification_dict['h_3'] + 1,
                cls_2_prp=self.class_2_threshold_prp
            )
            grid_final, point_final = gran.execute()
            grid_diss = self.join_and_dissolve(grid_final, cell_125)
            GLOBAL_GRID_LIST.append(grid_diss)
            GLOBAL_POINT_LIST.append(point_final)

        grid, water, df, df_non_empty = self.concat_and_save(GLOBAL_GRID_LIST,
                                                             GLOBAL_POINT_LIST,
                                                             self.outpath,
                                                             self.outlayer,
                                                             self.outcsv,
                                                             self.gdf_pts,
                                                             self.gdf_125m)
        return grid, water, df, df_non_empty

    def join_and_dissolve(self, grid_final, cell_125):
        """Dissolve grids based on dissolve id"""
        cell_125.set_index('GridID125m', inplace=True)
        grid_joined = cell_125.join(
            grid_final.set_index('ID125m')).reset_index()
        grid_diss = grid_joined.copy()[
            ['p', 'h', 'dissolve_id', 'geometry']].dissolve(by='dissolve_id',
                                                            aggfunc='sum')
        grid_diss = grid_diss.copy()[grid_diss.p > 0]
        return grid_diss

    def concat_and_save(self,
                        global_grid_list,
                        global_point_list,
                        out_file,
                        out_layer,
                        out_csv,
                        points,
                        grid_125m):
        """Concatenates grid and point lists and saves to outfiles"""
        grid_final = gpd.GeoDataFrame(pd.concat(global_grid_list),
                                      crs=27700).reset_index()
        point_final = pd.concat(global_point_list)
        point_final = gridgran.calculate_dist_point_moved(point_final, points,
                                                          grid_125m)
        point_final_removed = gridgran.make_point_df_removing_grids(
            point_final)
        # point_final_removed.to_csv(out_csv.parent.joinpath(
        #     f'{self.outlayer}_points_without_empty_grids.csv'),
        #     index=False)
        # point_final.to_csv(out_csv, index=False)
        # Need to choose the correct method to replace values
        grid_final = gridgran.check_for_below_threshold(
            grid_final,
            self.classification_dict['p_3'],
            self.classification_dict['h_3'],
            replace_with=self.fill_values_below_threshold_with)
        grid_final.rename(columns={"dissolve_id": "GridID"}, inplace=True)
        grid_final['pop_density'] = grid_final.p / grid_final.geometry.area
        # grid_final.to_file(out_file, layer=out_layer, driver='GPKG',
        #                    index=False)
        grid_to_clip = self.gdf_125m[~self.gdf_125m.GridID125m.isin(
            grid_final.GridID.tolist())]

        water_gdf = self.gdf_water_clip.copy()
        grid_125m_water = gridgran.remove_water_cells(
            grid_to_clip,
            water_gdf,
            return_water=True,
            index_col='GridID125m'
        )
        # if not grid_125m_water.empty:
        #     grid_125m_water.to_file(
        #         self.outpath,
        #         layer=f'{self.outlayer}_water_mask',
        #         driver='GPKG'
        #     )
        return grid_final, grid_125m_water, point_final, point_final_removed
