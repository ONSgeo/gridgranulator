"""Main function to prepare points from a global dataset at extent of local
authority and process at this extent. Although this will work for multuple
LAs, it may run out of memory for very large areas, and in these cases Local Authorities should be run separately."""
from pathlib import Path

import geopandas as gpd

import gridgran

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(r'D:\DATA\grids_dummy_data').resolve()
BFC_ALL = DATA_DIR.joinpath('BFC/CTRY_DEC_2021_GB_BFC.shp')
OA_SHP = DATA_DIR.parent.joinpath(
    'OA_2021/Output_Areas_(December_2021)_Boundaries_Full_Clipped_EW_(BFC)/OA_2021_EW_BFC_V7.shp')

LA_SHP = DATA_DIR.joinpath('Local_Authority_Districts_('
                          'December_2021)_GB_BFC/LAD_DEC_2021_GB_BFC.shp')

GLOBAL_POINTS = DATA_DIR.joinpath('DUMMY_POINTS_GLOBAL.gpkg')
GLOBAL_POINTS_LAYER = 'part-0'
GLOBAL_GRID_1km = DATA_DIR.joinpath("EWGRID_1km.gpkg")
GLOBAL_GRID_125m = Path(r'R:\HeatherPorter\CensusGrids\Nested '
                  r'Grids\NestedGridData\UKGrids\UKGrid_125m.gpkg').resolve()


classification_dict = {
        'p_1': 10*2,
        'p_2': 40*2,
        'p_3': 49*2,
        'h_1': 5*2,
        'h_2': 20*2,
        'h_3': 24*2,
        }

CLASSIFICATION_SETTINGS = {
    "classification_dict": classification_dict,
    "cls_2_threshold_1000m": False, # These should remain false
    "cls_2_threshold_500m": False, # These should remain false
    "cls_2_threshold_250m": False, # These should remain false
    "cls_2_threshold_125m": False, # These should remain false
}

def main(GPKG, la_ids, la_col):
    BFC_CLIP = GPKG.parent.joinpath('BFC_clip.shp')
    if not GPKG.parent.exists():
        GPKG.parent.mkdir()
    if not GPKG.exists():
        gridgran.make_points_geopackage(
            LA_SHP,
            la_ids,
            la_col,
            GLOBAL_POINTS,
            GPKG,
            GLOBAL_GRID_1km,
            GLOBAL_GRID_125m,
            out_layer='points',
            la_layer=None,
            pt_layer=GLOBAL_POINTS_LAYER,
            pt_pop_col='people',
            layer_1km='1km2',
            layer_125m=None
        )
    gridgran.clip_water(BFC_ALL, BFC_CLIP, GPKG, layer='1000m')
    add_oas_to_gpkg(GPKG)
    gridgran.GridGranulatorGPKG(GPKG,
                                GPKG,
                                GPKG.parent.name,
                                GPKG.parent.joinpath(f'{GPKG.parent.name}.csv'),
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
    #oa_join.drop_duplicates(subset='geometry', keep='first', inplace=True)
    oa_agg = oa_join[['OA21CD', 'people']].groupby('OA21CD').sum()
    #oa_final = oa_df.set_index('OA21CD').join(oa_agg).fillna(0)
    oa_final = oa_df.set_index('OA21CD').join(oa_agg).dropna()
    oa_final['pop_density'] = oa_final.people / oa_final.geometry.area
    oa_final.to_file(GPKG, layer='OA', driver='GPKG')


if __name__ == "__main__":
    from datetime import datetime
    start = datetime.now()
    # la_ids = ["Exeter", "East Devon", "North Somerset", "Torbay",
    #           "Plymouth", "Dorset", "Bournemouth", "Cornwall", "South "
    #                                                            "Somerset",
    #           "Sedgemoor", "Somerset West and Taunton", "Mendip",
    #           "West Devon", "Torridge", "Teignbridge", "South Hams", "North "
    #                                                                  "Devon", "Mid Devon"]
    LA_IDS = {
        # 'SOUTH': ["Southampton", "New Forest"],
        # 'BRISTOL_CARDIFF': ['Bristol, City of', 'North Somerset', 'Vale of '
        #                                                           'Glamorgan', 'Cardiff'],
        # 'Exeter': ['Exeter', 'Teignbridge'],
        # 'Leeds': ['Leeds'],
    #     'London': ['City of London',
    #                  'Barking and Dagenham',
    #                  'Barnet',
    #                  'Bexley',
    #                  'Brent',
    #                  'Bromley',
    #                  'Camden',
    #                  'Croydon',
    #                  'Ealing',
    #                  'Enfield',
    #                  'Greenwich',
    #                  'Hackney',
    #                  'Hammersmith and Fulham',
    #                  'Haringey',
    #                  'Harrow',
    #                  'Havering',
    #                  'Hillingdon',
    #                  'Hounslow',
    #                  'Islington',
    #                  'Kensington and Chelsea',
    #                  'Kingston upon Thames',
    #                  'Lambeth',
    #                  'Lewisham',
    #                  'Merton',
    #                  'Newham',
    #                  'Redbridge',
    #                  'Richmond upon Thames',
    #                  'Southwark',
    #                  'Sutton',
    #                  'Tower Hamlets',
    #                  'Waltham Forest',
    #                  'Wandsworth',
    #                  'Westminster']
    #
        # 'Southampton_100': ['Southampton'],
        # 'Exeter_100': ['Exeter'],
        # 'Birmingham_100': ['Birmingham'],
        # # 'Bristol': ['Bristol'],
        # 'Cardiff_100': ['Cardiff'],
        # 'New Forest_100': ['New Forest'],
        # 'Dorset_100': ['Dorset'],
        'Bristol_100': ['Bristol']
    }
    #la_ids = ["Southampton", "New Forest"]
    #la_ids = ['Southampton']
    la_col = 'LAD21NM' #Could also use LAD21CD
    for la, la_ids in LA_IDS.items():
        print(f'starting {la}')
        print(la)
        print(la_ids)
        GPKG = DATA_DIR.joinpath(f'{la}/{la}.gpkg')
        if not GPKG.parent.exists():
            GPKG.parent.mkdir()
        try:
            main(GPKG, la_ids, la_col)
        except Exception as e:
            print(f'Could not do {la} because of {e}')
        finish = datetime.now()
        print(f'{la} took {finish - start}')






