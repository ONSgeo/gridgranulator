from pathlib import Path
import geopandas as gpd
import fiona
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import os

from datetime import datetime

import gridgran

NUM_WORKERS = os.cpu_count()
BASE = Path(r'D:\DATA\grids_dummy_data').resolve()
GRID_1km = BASE.joinpath('EWGRID_1km.gpkg')
#GRID_1km = BASE.joinpath('Brighton/GRIDS_brighton.gpkg')
GRID_125M = Path(r'R:\HeatherPorter\CensusGrids\Nested '
                 r'Grids\NestedGridData\UKGrids').joinpath('UKGrid_125m.gpkg')
POINTS = BASE.joinpath('DUMMY_POINTS_GLOBAL.gpkg')
BFC_ALL = BASE.joinpath('BFC/CTRY_DEC_2021_GB_BFC.shp') # To make water
# mask

#OUTPATH = BASE.joinpath('brighton_parallel/TEST_brighton.gpkg')
#OUTPATH_TMP = OUTPATH.parent.joinpath('tmp/TEST_brighton.gpkg')

OUTPATH = BASE.joinpath('ew_parallel/EW.gpkg')
OUTPATH_TMP = OUTPATH.parent.joinpath('tmp/EW.gpkg')


classification_dict = {
        'p_1': 10,
        'p_2': 40,
        'p_3': 49,
        'h_1': 5,
        'h_2': 20,
        'h_3': 24,
        }

CLASSIFICATION_SETTINGS = {
    "classification_dict": classification_dict,
    "cls_2_threshold_1000m": False, # These should remain false
    "cls_2_threshold_500m": False, # These should remain false
    "cls_2_threshold_250m": False, # These should remain false
    "cls_2_threshold_125m": False, # These should remain false
}


touching_points_list = []

def main_process_serial(layer=None):
    m = multiprocessing.Manager()
    l_pt = m.Lock()
    l_125 = m.Lock()
    if layer:
        gdf_1km = gpd.read_file(GRID_1km, layer=layer)
    else:
        gdf_1km = gpd.read_file(GRID_1km)
    for grid_cell in gdf_1km.itertuples():
        process(grid_cell, l_pt, l_125)
    print(touching_points_list)

def main_process_parallel(layer=None):
    m = multiprocessing.Manager()
    l_pt = m.Lock()
    l_125 = m.Lock()
    l_water = m.Lock() #lock to read water mask file
    if layer:
        gdf_1km = gpd.read_file(GRID_1km, layer=layer)
    else:
        gdf_1km = gpd.read_file(GRID_1km)
    gdf_water_all = gpd.read_file(BFC_ALL)
    CELLS_1km = [x for index, x in gdf_1km.iterrows()]
    number_of_rows = len(CELLS_1km)
    rows_10_perc = round(number_of_rows/10)
    number_of_rows_left = len(CELLS_1km)
    counter = 0
    GRIDS = []
    WATERS = []
    DFS = []
    DFS_NON_EMPTY = []
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        future_to_grids = {executor.submit(process, cell, l_pt, l_125,
                                           l_water, gdf_water_all): cell
                           for
                           cell in CELLS_1km}
        for future in as_completed(future_to_grids):
            processed_grid = future_to_grids[future]
            try:
                grid, water, df, df_non_empty = future.result()
                if isinstance(grid, gpd.GeoDataFrame):
                    GRIDS.append(grid)
                    WATERS.append(water)
                    DFS.append(df)
                    DFS_NON_EMPTY.append(df_non_empty)
                if number_of_rows_left % rows_10_perc == 0:
                    print('ROWS LEFT', number_of_rows_left)
                number_of_rows_left -= 1
                counter += 1
                if counter % 500 == 0:
                    print(f'Processed {counter} cells')
            except Exception as e:
                print(processed_grid, e)
    grid_final = gpd.GeoDataFrame(pd.concat(GRIDS)).set_crs(27700)
    grid_final.to_file(OUTPATH, layer='grids',
                       driver='GPKG',
                      index=False)
    df_final = pd.concat(DFS)
    df_final.to_csv(OUTPATH.parent.joinpath('grids.csv'), index=False)
    df_non_empty_final = pd.concat(DFS_NON_EMPTY)
    df_non_empty_final.to_csv(OUTPATH.parent.joinpath(
        'points_in_non_empty_grids.csv'), index=False)
    df_water = gpd.GeoDataFrame(pd.concat(WATERS)).set_crs(27700)
    df_water['diss'] = 1
    df_water = df_water.dissolve(by='diss')
    df_water.to_file(OUTPATH, layer='watermask', index=False)




def process(grid_cell, l_pt, l_125, l_water, gdf_water_all):
    outlayer = grid_cell.GridID1km
    bbox = grid_cell.geometry.bounds
    #l_pt.acquire()
    points = gpd.read_file(POINTS, bbox=bbox)
    #l_pt.release()
    if not points.empty:
        #l_125.acquire()
        gdf_125 = gpd.read_file(GRID_125M, bbox=bbox)
        #l_125.release()
        gdf_125 = gdf_125[gdf_125.GridID125m.str[:-3] + '000' == grid_cell.GridID1km]
        touching_points = points[points.geometry.touches(grid_cell.geometry)]
        if not touching_points.empty:
            for pt in touching_points.uprn.tolist():
                if pt in touching_points_list:
                    points = points[points.uprn != pt]
                else:
                    touching_points_list.append(pt)
        #l_water.acquire()
        #water_gdf = gpd.read_file(BFC_ALL, bbox=bbox)
        water_gdf = gpd.clip(gdf_water_all, gdf_125)
        #l_water.release()
        x = gridgran.GridGranulatorSingleCell(
            grid_cell,
            gdf_125,
            points,
            water_gdf,
            OUTPATH_TMP,
            outlayer,
            OUTPATH_TMP.parent.joinpath(f'{outlayer}_grids.csv'),
            CLASSIFICATION_SETTINGS,
            class_2_threshold_prp=0.05,
            fill_values_below_threshold_with='minimum'

        )
        grid, water, df, df_non_empty = x.iterate_and_process()
        return grid, water, df, df_non_empty
    return None, None, None, None

def make_final_csv(csv_list, outname, delete_files=False):
    df_list = [pd.read_csv(x) for x in csv_list]
    df_final = pd.concat(df_list)
    df_final.to_csv(outname, index=False)
    if delete_files:
        [x.unlink() for x in csv_list]

def concatenate_gpkg_layers(gpkg_tmp, out_gpkg, delete_tmp=False):
    # https://gis.stackexchange.com/questions/300630/how-to-use-merge-vector-layers-in-qgis-using-geopackages-as-output-with-fids-d
    layers = fiona.listlayers(gpkg_tmp)
    gdf_grids_list = []
    gdf_water_list = []
    for layer in layers:
        gdf = gpd.read_file(gpkg_tmp, layer=layer)
        if layer.endswith('water_mask'):
            gdf_water_list.append(gdf)
        else:
            gdf_grids_list.append(gdf)

    gdf_grids = gpd.GeoDataFrame(pd.concat(gdf_grids_list)).set_crs(27700)
    gdf_water = gpd.GeoDataFrame(pd.concat(gdf_water_list)).set_crs(27700)
    gdf_grids.to_file(out_gpkg, layer='grids', index=False)
    gdf_water['diss'] = 1
    gdf_water = gdf_water.dissolve(by='diss').reset_index()
    gdf_water.to_file(out_gpkg, layer='watermask', index=False)
    if delete_tmp:
        gpkg_tmp.unlink()


if __name__ == "__main__":
    #main_process_serial(layer='1000m')
    start = datetime.now()
    if not OUTPATH_TMP.parent.exists():
        OUTPATH_TMP.parent.mkdir(parents=True)
    #main_process_parallel(layer='1000m')
    main_process_parallel(layer='1km2')
    # GRIDS_CSVS = [x for x in OUTPATH_TMP.parent.iterdir() if x.name.endswith(
    #     '0_grids.csv')]
    # POINTS_CSVS = [x for x in OUTPATH_TMP.parent.iterdir() if x.name.endswith(
    #     'empty_grids.csv')]
    # make_final_csv(GRIDS_CSVS, OUTPATH.parent.joinpath('EW.csv'),
    #                delete_files=True)
    # make_final_csv(POINTS_CSVS, OUTPATH.parent.joinpath(
    #     'EW_points_without_empty_grids.csv'), delete_files=True)
    # concatenate_gpkg_layers(OUTPATH_TMP, OUTPATH, delete_tmp=True)
    # OUTPATH_TMP.parent.rmdir()
    print('FIND TOUCHING POINTS')
    print('APPEND CSVS AND LAYERS')
    print('DISSOLVE WATER')
    finish = datetime.now()
    print(f'parallel {finish - start} seconds')
    # print(r"https://stackoverflow.com/questions/6832554/multiprocessing-how\
    #                -do-i-share-a-dict-among-multiple-processes")
    # print(r"https://www.google.com/search?q=python+concurrent.futures+write+to+dict+from+multiple+processes&rlz=1C1GCEB_enGB1000GB1000&sxsrf=ALiCzsbs9sz168hf-WQ8mrK4RAXFD1buyQ%3A1666021331034&ei=03dNY-rUAYSQgQa4vaHACw&ved=0ahUKEwiqiv-Mzef6AhUESMAKHbheCLgQ4dUDCA4&uact=5&oq=python+concurrent.futures+write+to+dict+from+multiple+processes&gs_lcp=Cgdnd3Mtd2l6EAM6BggAEBYQHjoFCAAQhgM6BggAEB4QDToICAAQCBAeEA06BQghEKABOggIIRAWEB4QHToHCCEQoAEQCjoECCEQFToECCEQCkoECE0YAUoECEEYAEoECEYYAFAAWMc6YPE7aABwAXgAgAHnAYgByyCSAQcyNC4xMC4zmAEAoAEBwAEB&sclient=gws-wiz")
