from pathlib import Path
from setuptools import find_packages
from setuptools import setup

DEPS = Path(__file__).resolve().parent.joinpath('dependencies')

rtree = str(DEPS.joinpath('Rtree-0.9.7-cp38-cp38-win_amd64.whl'))
gdal = str(DEPS.joinpath('GDAL-3.2.2-cp38-cp38-win_amd64.whl'))
fiona = str(DEPS.joinpath('Fiona-1.8.18-cp38-cp38-win_amd64.whl'))
pyproj = str(DEPS.joinpath('pyproj-3.0.1-cp38-cp38-win_amd64.whl'))
shapely = str(DEPS.joinpath('Shapely-1.8a3-cp38-cp38-win_amd64.whl'))



setup(
    name='gridgran',
    version='1.0.0',
    description='Package to disseminate census point data using hierarchical grids with the aim of showing as much spatial detail as possible whilst not breaking disclosure rules',
    authoer='David Kerr',
    author_email='david.kerr@ons.gov.uk',
    url='https://github.com/ONSgeo/gridgranulator',
    packages=find_packages(exclude=['tests*']),

)