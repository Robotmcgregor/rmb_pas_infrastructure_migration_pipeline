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
from shutil import ignore_patterns
import geopandas as gpd
import fiona

def get_schema_fn(path_):
    """
    :param path_: string object containing the path to a shapefile
    :return: dictionary object containing the schema of the passed shapefile
    """
    gdf_orig = fiona.open(path_)
    schema = gdf_orig.schema

    return schema


# def mk_dir(path_):
#     """
#     Check if a directory exists; if not, create it.
#     :param path_: string object containing the path of the directory.
#     """
#     if not os.path.isdir(path_):
#         os.mkdir(path_)

def main_routine(source_dir, export_dir):
    """
    This script creates a backup copy of the Lib_Corporate, Archive, Other_Data and Property_Edits directories and stores
    them in the U drive Pastoral Infrastructure Backup directory.
    """
    print('Overwriting pastoral infrastructure shapefiles is in progress........')

    type_list = ["Points", "Lines", "Polys_Paddocks", "Polys_Other"]

    source_lib_corp = os.path.join(source_dir, "Lib_Corporate", "Data", "ESRI")
    export_lib_corp = os.path.join(export_dir, "Lib_Corporate", "Data", "ESRI")

    source_archive = os.path.join(source_dir, "Archive", "Data", "ESRI")
    export_archive = os.path.join(export_dir, "Archive", "Data", "ESRI")

    source_pdf = os.path.join(source_dir, "Lib_Corporate", "Products", "Maps")
    export_pdf = os.path.join(export_dir, "Lib_Corporate", "Products", "Maps")

    source_arc_pdf = os.path.join(source_dir, "Archive", "Maps", "A0_MonSites")
    export_arc_pdf = os.path.join(export_dir, "Archive", "Maps", "A0_MonSites")

    for file_name in type_list:

        # --------------------------------------------- Lib Corporate --------------------------------------------------
        input_path = os.path.join(source_lib_corp, "Pastoral_Infra_{0}.shp".format(file_name))
        if os.path.isfile(input_path):
            print("- Located: ", input_path)

            # call the get_schema function to export the required shapefile schema
            corp_schema = get_schema_fn(input_path)
            gdf = gpd.read_file(input_path, driver="ESRI Shapefile")
            export_path = os.path.join(export_lib_corp, "Pastoral_Infra_{0}.shp".format(file_name))

            gdf.to_file(export_path, driver="ESRI Shapefile", schema=corp_schema)
            print("file copied: ", export_path)

        else:
            print("Error - file not located: ", input_path)


        # ------------------------------------------------ Archive -----------------------------------------------------

        input_path = os.path.join(source_archive, "Pastoral_Infra_{0}.shp".format(file_name))
        if os.path.isfile(input_path):
            print("- Located: ", input_path)

            # call the get_schema function to export the required shapefile schema
            corp_schema = get_schema_fn(input_path)

            gdf = gpd.read_file(input_path, driver="ESRI Shapefile")
            export_path = os.path.join(export_archive, "Pastoral_Infra_{0}.shp".format(file_name))

            gdf.to_file(export_path, driver="ESRI Shapefile", schema=corp_schema)
            print("file copied: ", export_path)

        else:
            print("Error - file not located: ", input_path)


    for pdf_path in glob(os.path.join(source_pdf, "*.pdf")):

        head_tail = os.path.split(pdf_path)
        export_pdf_path = os.path.join(export_pdf, head_tail[1])

        try:
            shutil.copy(pdf_path, export_pdf_path)
            print("coppied: ", export_pdf_path)

        # If there is any permission issue
        except PermissionError:
            print("ERROR ---- "*10)
            print("Permission denied: ", export_pdf)
            print("File open - this will need to be moved manually")
            
            
    for pdf_path in glob(os.path.join(source_arc_pdf, "*.pdf")):

        head_tail = os.path.split(pdf_path)
        export_pdf_path = os.path.join(export_pdf, head_tail[1])

        try:
            shutil.copy(pdf_path, export_pdf_path)
            print("coppied: ", export_pdf_path)

        # If there is any permission issue
        except PermissionError:
            print("ERROR ---- "*10)
            print("Permission denied: ", export_arc_pdf)
            print("File open - this will need to be moved manually")



if __name__ == '__main__':
    main_routine()