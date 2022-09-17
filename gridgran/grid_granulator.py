"""
Main entry class into package that takes all of the arguments and processes
the data according to this
"""

import geopandas as gpd
import pandas as pd
from pathlib import Path

import gridgran


class GridGranulatorGPKG:
    """Class to take input paths and parameters, iterate over 1km grids and
    process data"""

    def __init__(self,
                 gpkg_path,
                 out_path,
                 out_layer,
                 out_csv,
                 classification_settings,
                 path_to_waterline=None,
                 class_2_threshold_prp=0.05,
                 fill_values_below_threshold_with='minimum'):
        """ Initialisation

        Parameters:
        -----------
        gpkg_path : Path/str
            Path to geopackage containing 1000m and 125m grids, as well as
            points layer. The names of these
            layers should be '1000m', '125m' and 'points' (if present)

        out_path : Path/str
            Path to geopackage to which output should be save (it can be the \
             same as gpkg_path)

        out_layer : str
            Output layer within geopackage

        out_csv : Path/str
            Output csv path

        classification_settings : dict

        path_to_waterline : Path/str/None
            Path to BFC highwater vector layer for UK. If this is missing,
            coastline will not be clipped, else coastline will be clipped
            where 125m grids intersect this level - (Default=None)

        class_2_threshold : float
            If grid cells classified as class 2 are below this proportion of \
             the total population within neighbouring cells within parent
             cell, they will be reclassified as class 1 - else all cells
             will  be aggregated up. This is to stop class 2 cells causing
             densely populated cells within parent cells being aggregated up \
              to parent level.

        fill_values_below_threshold_with : str
        Options ['minimum', 'star', 'null'] -
        In the output grid, cells that finish below the minimum threshold
        will need to be filled with a dummy value so as no to identify
        individuals. The options are 'minimum' (the minimum threshold value),
        'star' (asterisk '*') or 'null'/NA. (Default='minimum')
        """
        self.gpkg_path = Path(gpkg_path).resolve()
        self.out_path = Path(out_path).resolve()
        self.out_layer = out_layer
        self.out_csv = Path(out_csv).resolve()
        self.classification_settings = classification_settings
        self.classification_dict = self.classification_settings[
            "classification_dict"]
        if path_to_waterline:
            self.path_to_waterline = Path(path_to_waterline).resolve()
        else:
            self.path_to_waterline = None
        self.class_2_threshold_prp = class_2_threshold_prp
        self.fill_values_below_threshold_with =  \
            fill_values_below_threshold_with
        self.grid_1km, self.grid_125m, self.points = \
            self.get_points_and_1km_and_125m()
        self.iterate_and_process()


    def get_points_and_1km_and_125m(self):
        """Returns geodataframes for grids and points"""
        grid_1km = gpd.read_file(self.gpkg_path,
                                 layer='1000m')
        grid_125m = gpd.read_file(self.gpkg_path,
                                  layer='125m')
        points = gpd.read_file(self.gpkg_path, layer='points')
        grid_1km = grid_1km.to_crs(27700)
        grid_125m = grid_125m.to_crs(27700)
        return grid_1km, grid_125m, points

    def iterate_and_process(self):
        pops = 0
        GLOBAL_GRID_LIST = []
        GLOBAL_POINT_LIST = []
        df_grid_125, df_grid_pt = \
            gridgran.prep_points_and_grid_from_dataframes(
                self.grid_125m,
                self.points,
                self.classification_dict,
                self.class_2_threshold_prp)
        for row in self.grid_1km.itertuples():
            cell_125 = self.grid_125m[
                self.grid_125m.GridID125m.str.startswith(row.GridID1km[:-3])]
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

        self.concat_and_save(GLOBAL_GRID_LIST,
                             GLOBAL_POINT_LIST,
                             self.out_path,
                             self.out_layer,
                             self.out_csv,
                             self.points,
                             self.grid_125m)

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
        point_final_removed.to_csv(out_csv.parent.joinpath('removed.csv'),
                                   index=False)
        point_final.to_csv(out_csv, index=False)
        # Need to choose the correct method to replace values
        grid_final = gridgran.check_for_below_threshold(
            grid_final,
            self.classification_dict['p_3'],
            self.classification_dict['h_3'],
            replace_with=self.fill_values_below_threshold_with)
        grid_final.rename(columns={"dissolve_id": "GridID"}, inplace=True)
        grid_final.to_file(out_file, layer=out_layer, driver='GPKG',
                           index=False)
        print(grid_final)
        print(self.grid_125m)
        grid_to_clip = self.grid_125m[~self.grid_125m.GridID125m.isin(
            grid_final.GridID.tolist())]
        print(grid_to_clip)
        if self.path_to_waterline:
            water_gdf = gpd.read_file(self.path_to_waterline)
            grid_125m_water = gridgran.remove_water_cells(
                grid_to_clip,
                water_gdf,
                return_water=True,
                index_col='GridID125m'
            )
            print(grid_125m_water)
            grid_125m_water.to_file(
                self.out_path,
                layer='water_mask',
                driver='GPKG'
            )
