#!/usr/bin/env python
"""
This script automates the back of the pastoral infrastructure database and map drives.
Author: Rob McGregor
Date: 04/11/2021

"""
# from __future__ import print_function, division

# import the requried modules
# import sys
# import os
# import argparse
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
    source_dir = r"U:\Pastoral_Infrastructure"

    # extract the current year of running script
    year = datetime.today().year
    print(year)

    # create a directory
    primary_dir = os.path.join(source_dir, "Annual_Snapshot")
    mk_dir(primary_dir)

    # create a directory
    year_dir = os.path.join(primary_dir, str(year))
    mk_dir(year_dir)

    # create source directory path
    source_corp = os.path.join(source_dir, "Lib_Corporate", "Data", "ESRI")

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