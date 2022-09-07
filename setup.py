from setuptools import find_packages
from setuptools import setup

setup(
    name='gridgran',
    version='1.0.0',
    description='Package to disseminate census point data using hierarchical \
     grids with the aim of showing as much spatial detail as possible whilst \
      not breaking disclosure rules',
    authoer='David Kerr',
    author_email='david.kerr@ons.gov.uk',
    url='https://github.com/ONSgeo/gridgranulator',
    packages=find_packages(exclude=['tests*']),
)
