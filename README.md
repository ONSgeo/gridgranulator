# gridgranulator

## Anonymising disclosive geospatial data – granular grids
Python package to aid the uniform disaggregation and dissemination of census data that masks the individual

### Background
Whilst tabular and geospatial census outputs are produced at the individual level, they cannot be disseminated at this resolution due to their disclosive nature. Census data are usually disseminated as aggregations to the geographies in which the individual data lie; most coarsely at the national-level, and most detailed at the census output area. To increase resolution and uniformity, these data have also been disaggregated and disseminated in 1km grids where individual data are aggregated up to 1km cells that align, and nest within, the British National grid. Although these grids are useful when estimating population with proportions of output areas, particularly in cases of large output areas, when analysing population over small, densely populated areas, there can be some imprecision due to a lack of spatial variation in population distributions, both in 1km gridded data, and census output areas. For instance, if population is being summed within the catchment area of a particular service location, values can vary markedly between the true census individual-level population, the 1km grids and the census output areas.
There are a number of methods that can be used to increase the spatial resolution of unit-based census data using dasymetric disaggregation, but these models are a function of the input covariates, and their accuracy with respect to real values and locations decreases with an increase of resolution (i.e. the closer the resolution of the grids are to those of the census units, the higher the accuracy of the individual grid cells’ values with respect to true locations/values counted in the census, and vice versa).

### ONS Census Grids
The Geospatial Team in Digital Growth and Operations have developed a method to aggregate disclosive point data to hierarchical grids based on their density and underlying population. The code written takes point data to be aggregated, along with rules upon which the hierarchical grids will be based, and will output a geopackage containing layers for the hierarchical grids (with fields for population and household count and population density per grid cell), corresponding Output Areas with the same fields, a high-water mask, as well as the input points, and the 1km and 125m grids corresponding to the extents of the input grids. It will also output csv tables for each point spatially joined to a grid cell (as well as empty 125m grids cells), with fields to indicate their UPRN code (if populated), the 125m grid cell ID, the grid cell’s parent IDs (250m, 500m, 1km), as well as a record of 125m cells in which the point was located at each level of aggregation (500m, 250m, 125m) and the distance that the point travelled in cases where points were required to be used. There is another csv table that shows the same data, but with the unpopulated grid cells omitted. See Output csv descriptions.

## Gridgranulator Python Package

### Workflow
Upon instantiation of an GridGranulatorGPKG object, the code will automate
the code based on the input points, their underlying population counts, and
the following thresholds in the classification_dict:

```
p_1 -> Population class 1 - Population > 0  <= p_1
p_2 -> Population class 2 - Population > p_1 <= p_2
p_3 -> Population class 3 - Population > p_2 <= p_3
```
The code will then carry out the following functionality
