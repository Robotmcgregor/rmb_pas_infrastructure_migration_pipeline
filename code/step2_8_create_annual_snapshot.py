#!/usr/bin/env python
"""
### Description ###
This script creates a snapshot of pastral infrstuture per year ie between 001-01-{year} until 31-12-{year}.

Copyright 2021 Robert McGregor

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


Author: Robert McGregor
Date: 04/11/2021
Email: robert.mcgregor@nt.gov.au

"""
#

# import the required modules
from __future__ import print_function, division
import shutil
from datetime import datetime
from glob import glob
import os
import geopandas as gpd


def mk_dir(path_):
    """
    Check if a directory exists; if not, create it.
    :param path_: string object containing the path of the directory.
    """
    if not os.path.isdir(path_):
        os.mkdir(path_)

def main_routine(pastoral_infrastructure):
    """
    This script creates a backup copy of the Lib_Corporate, Archive, Other_Data and Property_Edits directories and stores
    them in the U drive Pastoral Infrastructure Backup directory.
    """
    print('Annual snapshot is in progress........')

    # extract the current year of running script
    year = datetime.today().year
    print(year)

    # create a directory
    primary_dir = os.path.join(pastoral_infrastructure, "Annual_Snapshot")
    mk_dir(primary_dir)

    # create a directory
    year_dir = os.path.join(primary_dir, str(year))
    mk_dir(year_dir)

    # create source directory path
    source_corp = os.path.join(pastoral_infrastructure, "Lib_Corporate", "Data", "ESRI")

    # search for shapefiles, add a column with the year and export to the newly created directory.
    for file_ in glob(os.path.join(source_corp, "*.shp")):
        gdf = gpd.read_file(file_)
        gdf["SNAP_SHT"] = str(year)
        head_tail = os.path.split(file_)

        output_file = os.path.join(year_dir, "{0}_{1}".format(str(year), head_tail[1]))

        if os.path.isfile(output_file):
            os.remove(output_file)
            gdf.to_file(output_file, driver="ESRI Shapefile")

        else:
            gdf.to_file(output_file, driver="ESRI Shapefile")

if __name__ == '__main__':
    main_routine()