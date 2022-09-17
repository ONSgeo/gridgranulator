from pathlib import Path

import gridgran

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.joinpath('tests/data')
GPKG = DATA_DIR.joinpath('GRIDS.gpkg')
BFC = DATA_DIR.joinpath('waterline/BFC.shp')

classification_dict = {
        'p_1': 10,
        'p_2': 40,
        'p_3': 49,
        'h_1': 5,
        'h_2': 20,
        'h_3': 24,
        }

CLASSIFICATION_SETTINGS = {
    "classification_dict" : classification_dict,
    "cls_2_threshold_1000m": False,
    "cls_2_threshold_500m": False,
    "cls_2_threshold_250m": False,
    "cls_2_threshold_125m": False,
}

def main():
    gridgran.GridGranulatorGPKG(GPKG,
                                GPKG,
                                'example',
                                GPKG.parent.joinpath('example.csv'),
                                CLASSIFICATION_SETTINGS,
                                path_to_waterline=BFC,
                                class_2_threshold_prp=0.05,
                                fill_values_below_threshold_with='minimum'
                                )


if __name__ == "__main__":
    import datetime
    start = datetime.datetime.now()
    main()
    end = datetime.datetime.now()
    print(f'It took {end - start} seconds')