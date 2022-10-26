from pathlib import Path
import geopandas as gpd
from concurrent.futures import ProcessPoolExecutor, as_completed

import gridgran

BASE = Path(r'D:\DATA\grids_dummy_data').resolve()
#GRID_1km = BASE.joinpath('UKGRID_1km.gpkg')
GRID_1km = BASE.joinpath('Brighton/GRIDS_brighton.gpkg')
GRID_125M = Path(r'R:\HeatherPorter\CensusGrids\Nested '
                 r'Grids\NestedGridData\UKGrids').joinpath('UKGrid_125m.gpkg')
POINTS = BASE.joinpath('DUMMY_POINTS_GLOBAL.gpkg')

touching_points_list = []

def main_process_serial(layer=None):
    if layer:
        gdf_1km = gpd.read_file(GRID_1km, layer=layer)
    else:
        gdf_1km = gpd.read_file(GRID_1km)
    for grid_cell in gdf_1km.itertuples():
        process(grid_cell)
    print(touching_points_list)


def process(grid_cell):
    bbox = grid_cell.geometry.bounds
    points = gpd.read_file(POINTS, bbox=bbox)
    if not points.empty:
        gdf_125 = gpd.read_file(GRID_125M, bbox=bbox)
        gdf_125 = gdf_125[gdf_125.GridID125m.str[:-3] + '000' == grid_cell.GridID1km]
        touching_points = points[points.geometry.touches(grid_cell.geometry)]
        if not touching_points.empty:
            for pt in touching_points.uprn.tolist():
                if pt in touching_points_list:
                    points = points[points.uprn != pt]
                else:
                    touching_points_list.append(pt)






if __name__ == "__main__":
    main_process_serial(layer='1000m')
    # print(r"https://stackoverflow.com/questions/6832554/multiprocessing-how\
    #                -do-i-share-a-dict-among-multiple-processes")
    # print(r"https://www.google.com/search?q=python+concurrent.futures+write+to+dict+from+multiple+processes&rlz=1C1GCEB_enGB1000GB1000&sxsrf=ALiCzsbs9sz168hf-WQ8mrK4RAXFD1buyQ%3A1666021331034&ei=03dNY-rUAYSQgQa4vaHACw&ved=0ahUKEwiqiv-Mzef6AhUESMAKHbheCLgQ4dUDCA4&uact=5&oq=python+concurrent.futures+write+to+dict+from+multiple+processes&gs_lcp=Cgdnd3Mtd2l6EAM6BggAEBYQHjoFCAAQhgM6BggAEB4QDToICAAQCBAeEA06BQghEKABOggIIRAWEB4QHToHCCEQoAEQCjoECCEQFToECCEQCkoECE0YAUoECEEYAEoECEYYAFAAWMc6YPE7aABwAXgAgAHnAYgByyCSAQcyNC4xMC4zmAEAoAEBwAEB&sclient=gws-wiz")

