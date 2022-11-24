"""Processing scipt that takes a csv of point with the following fields

UPRN, OA21CD, UPRN_POPULATION, BNG_NORTHING, BNG_EASTING

The script will prepare the points to have grids processed. THE USER MUST
ALSO INCLUDE THE LA(S) IN WHICH THE POINTS LIE FOR THIS TO WORK


PATHS SHOULD BE CHECKED BEFORE RUNNING CODE

"""

from pathlib import Path
import geopandas as gpd
import pandas as pd
import gridgran



# DATA_DIR = Path(r'D:\DATA\grids_dummy_data').resolve()
DATA_DIR = Path(r'Q:\Census_grids_data_DO_NOT_DELETE').resolve()
BFC_ALL = DATA_DIR.joinpath('BFC/CTRY_DEC_2021_GB_BFC.shp')  # FOR WATER MASK
OA_SHP = DATA_DIR.joinpath(
    'OA_2021/Output_Areas_(December_2021)_Boundaries_Full_Clipped_EW_('
    'BFC)/OA_2021_EW_BFC_V7.shp')

LA_SHP = DATA_DIR.joinpath('Local_Authority_Districts_('
                           'December_2021)_GB_BFC/LAD_DEC_2021_GB_BFC.shp')

GLOBAL_GRID_1km = DATA_DIR.joinpath("EWGRID_1km.gpkg")
GLOBAL_GRID_125m = Path(r'R:\HeatherPorter\CensusGrids\Nested '
                        r'Grids\NestedGridData\UKGrids\UKGrid_125m.gpkg'
                        ).resolve()

########################## SET OUT DIRECTORY ################################
OUT_DIR = Path(r"D:\DATA\grids_dummy_data\BOA").resolve()  # THIS SHOULD BE SET AS
# APPROPRIATE
########################## SET OUT DIRECTORY ################################


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
    "cls_2_threshold_1000m": False,  # These should remain false
    "cls_2_threshold_500m": False,  # These should remain false
    "cls_2_threshold_250m": False,  # These should remain false
    "cls_2_threshold_125m": False,  # These should remain false
}


def make_points(CSV, GPKG):
    df = pd.read_csv(CSV)
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.BNG_EASTING, df.BNG_NORTHING), crs=27700)
    gdf = gdf[['UPRN', 'UPRN_POPULATION', 'geometry']]
    gdf.rename(columns={'UPRN': 'uprn', 'UPRN_POPULATION': 'people'},
               inplace=True)
    gdf.to_file(GPKG, layer='points', index=False)


def main(GPKG, la_ids, la_col, include_oas=False):
    #If include_oas is False, the oa layer will not be included in the
    # output. This is just for purposes of comparison, but slows down the
    # script if data is on the network
    BFC_CLIP = GPKG.parent.joinpath('BFC_clip.shp')
    if not GPKG.parent.exists():
        GPKG.parent.mkdir()
    if not GPKG.exists():
        gridgran.make_points_geopackage(
            LA_SHP,
            la_ids,
            la_col,
            GPKG,
            GPKG,
            GLOBAL_GRID_1km,
            GLOBAL_GRID_125m,
            out_layer='points',
            la_layer=None,
            pt_layer='points',
            pt_pop_col='people',
            layer_1km='1km2',
            layer_125m=None
        )
    gridgran.clip_water(BFC_ALL, BFC_CLIP, GPKG, layer='1000m')
    if include_oas:
        add_oas_to_gpkg(GPKG)
    gridgran.GridGranulatorGPKG(GPKG,
                                GPKG,
                                GPKG.parent.name,
                                GPKG.parent.joinpath(
                                    f'{GPKG.parent.name}.csv'),
                                CLASSIFICATION_SETTINGS,
                                path_to_waterline=BFC_CLIP,
                                class_2_threshold_prp=0.05,
                                fill_values_below_threshold_with='minimum'
                                )


def add_oas_to_gpkg(GPKG):
    """Aggregates points to OA shapefile in gpkg"""
    pt_df = gpd.read_file(GPKG, layer='points')
    bbox = tuple(list(pt_df.total_bounds))
    oa_df = gpd.read_file(OA_SHP, bbox=bbox)
    oa_join = oa_df.sjoin(pt_df, how='inner', predicate='intersects')
    oa_agg = oa_join[['OA21CD', 'people']].groupby('OA21CD').sum()
    oa_final = oa_df.set_index('OA21CD').join(oa_agg).dropna()
    oa_final['pop_density'] = oa_final.people / oa_final.geometry.area
    oa_final.to_file(GPKG, layer='OA', driver='GPKG')






if __name__ == "__main__":
    CSV = OUT_DIR.joinpath('BOA_UPRN_OA_Population.csv')
    GPKG = OUT_DIR.joinpath('BOA.gpkg')
    make_points(CSV, GPKG)
    LA_IDS = ['Wiltshire']
    LA_COL = 'LAD21NM'
    if not GPKG.parent.exists():
        GPKG.parent.mkdir(parents=True, exist_ok=True)
    try:
        main(GPKG, LA_IDS, LA_COL, include_oas=False)
    except Exception as e:
        print(f'Could not do {LA_IDS} because of {e}')
