# gridgranulator

## Anonymising disclosive geospatial data – granular grids
Python package to aid the uniform disaggregation and dissemination of census data that masks the individual

### Background
Whilst tabular and geospatial census outputs are produced at the individual level, they cannot be disseminated at this resolution due to their disclosive nature. Census data are usually disseminated as aggregations to the geographies in which the individual data lie; most coarsely at the national-level, and most detailed at the census output area. To increase resolution and uniformity, these data have also been disaggregated and disseminated in 1km grids where individual data are aggregated up to 1km cells that align, and nest within, the British National grid. Although these grids are useful when estimating population with proportions of output areas, particularly in cases of large output areas, when analysing population over small, densely populated areas, there can be some imprecision due to a lack of spatial variation in population distributions, both in 1km gridded data, and census output areas. For instance, if population is being summed within the catchment area of a particular service location, values can vary markedly between the true census individual-level population, the 1km grids and the census output areas.
There are a number of methods that can be used to increase the spatial resolution of unit-based census data using dasymetric disaggregation, but these models are a function of the input covariates, and their accuracy with respect to real values and locations decreases with an increase of resolution (i.e. the closer the resolution of the grids are to those of the census units, the higher the accuracy of the individual grid cells’ values with respect to true locations/values counted in the census, and vice versa).

### ONS Census Grids
The Geospatial Team in Digital Growth and Operations have developed a method to aggregate disclosive point data to hierarchical grids based on their density and underlying population. The code written takes point data to be aggregated, along with rules upon which the hierarchical grids will be based, and will output a geopackage containing layers for the hierarchical grids (with fields for population and household count and population density per grid cell), corresponding Output Areas with the same fields, a high-water mask, as well as the input points, and the 1km and 125m grids corresponding to the extents of the input grids. It will also output csv tables for each point spatially joined to a grid cell (as well as empty 125m grids cells), with fields to indicate their UPRN code (if populated), the 125m grid cell ID, the grid cell’s parent IDs (250m, 500m, 1km), as well as a record of 125m cells in which the point was located at each level of aggregation (500m, 250m, 125m) and the distance that the point travelled in cases where points were required to be used. There is another csv table that shows the same data, but with the unpopulated grid cells omitted.

## Gridgranulator Python Package

### Workflow
Upon instantiation of an GridGranulatorGPKG object, the code will automate
the code based on the input points, their underlying population counts, and
the following thresholds in the classification_dict:

```
p_1 -> Population class 1 -   0 < Population  <= p_1
p_2 -> Population class 2 - p_1 < Population <= p_2
p_3 -> Population class 3 - p_2 < Population <= p_3
h_1 -> Population class 1 -   0 < No Households (points)  <= h_1
h_2 -> Population class 2 - p_1 < No Households (points) <= h_2
h_3 -> Population class 3 - p_2 < No Households (points) <= h_3
```
The non-disclosure population and household count then become p_3 + 1 and
h_3 + 1 respectively.

The code will then carry out the following functionality:
1. Spatially join points to 125m grid on intersection;
2. Iterate through cells in 1km grid;
3. Subset dataframe in 1. within grid cell from 2.
4. Iteratively split the parent cell (starting with 1km) into 4, and
classifying the 4 children cell based on the population classes in the
classification_dict. The classifications are actioned as follows:
 - Class 0 - Unpopulated - Grid cell is left as it is
 - Class 1 - Move elsewhere - Points in this cell are moved to class 3 or
 class 4 neighbours to bring them above disclosure limit
 - Class 2 - Fail - Cell and its neighbour is dissolved up to parent level
 IF Class 2 cell is above class_2_threshold_prp percent of total population
 within parent cell (see Optional Parameters). If below
 class_2_threshold_prp Class 2 cells are changed to Class 1 and passed to
 neighbouring cells.
 - Class 3 - Borrow from neighbours - Cell can try to bring itself above the
  threshold if possible. If not, Class 3 cell and its neighbours are
  dissolved up to parent level
 - Class 4 - Above disclosure limit - Cell can pass any points that are over
  threshold limit to neighbouring Class 3 cells. These points are selected
  randomly to prevent disclosure.
5. If the children cells within the parent cell can all be classified as
Class 0 or Class 4, the individual children cells can be split further until
 disclosure rules are no longer kept, at which point the grids are dissolved
  to their most non-disclosive level. If cells fail disclosure tests, the
  children are dissolved to the parent level, and the code moves on to the
  next cell in the iteration.
6. Once all 1km cells and their children have been processed, the grids are
dissolved to their non-disclosive level, and saved to geopackage.

In addition to the creation of the above grids, the package will also create
 a water mask snapped to the 125m cells, indicating the location of water
 with respect to the high-water mark.


### GridGranulatorGPKG Class
The main entry into the package is through the GridGranulatorGPKG Class.
Upon the creation of this class with the required parameters, the code will
automate the production of the grids. The parameters are as follows:

```
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
```



### Optional Parameters
``` path_to_waterline ``` - Path to CTRY_DEC_2021_GB_BFC.shp shapefile (for
example) to use as water mask. Please not that this will make the inverse of
 a terrestrial vector dataset to show where water intersects the 125m grid
 cells.

 ``` class_2_threshold ``` - Proportion below which Class 2 cells'
 population of the parents' population will be reclassified as Class 1.
 Value should be between 0 and 1. The higher the value, the more detailed
 (and disclosive)
 the grids.

 ``` fill_values_below_threshold_with ``` - Because tables cannot be shared
 that show disclosive values, any 1km cells that break disclosive threshold
 rules will be adjusted to show this value (either p_3 + 1 and h_3 + 1, an
 asterisk or 'null').

**NOTE - 1km cells that are adjusted using fill_values_below_threshold_with
 will result in table sums being different to those of the original data.
 Each row in the output table should be adjusted again following processing
 if you would like the sums to match the original point values.**

 **NOTE -  SHOULD USERS WISH TO MAKE POINT DATA NON-DISCLOSIVE ONLY GRIDS
 AND WATERMASKS IN THE OUTPUT GEOPACKAGES SHOULD BE SHARED. THE POINTS
 WITHIN THE GEOPACKAGES SHOULD BE DELETED AND CSV TABLES SHOULD NOT BE
 SHARED AS THESE CAN RESULT IN DISCLOSURE**


## Helper Scripts

### ./main.py
This script in the root of this repository can be used to automate the
production of test data internally using data in the ONS PSMA drive. The
user should specify the LA(s) they would like to process in the ``` if
__main__ == "__main__": ``` part of the script. User's should also specify
whether the outputs should include output areas for comparison in the output
 geopackage (include_oas=True). This OA data is currently held internally on
  the ONS network and runs slowly, so it might be beneficial to hold the OA
  data locally and change the paths in this script should the user want to
  make use of OAs. SEE OA_SHP variable at the top of the script.

### ./main_parallel.py
This script was created to run global data over large extents using multiple
 cpus. The code works, but may take some time if the user's computer does
 not offer enough cores OR this code needs to be made more efficient.

### ./example.py
This code is a simple example pointing to the test data in ./tests/data to
show how to run code on pre-built geopackages.


### TODO
1. **MAKE CODE THAT TAKES AN INPUT CSV OF POINTS AND RUNS THE CODE
AUTOMATICALLY**
2. setup.py for installation
3. Optimise main_parallel.py



