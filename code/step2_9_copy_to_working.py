#!/usr/bin/env python
"""
### Description ###
The RMB Pastoral Infrastructure Pipeline is the second half of the Pastoral Infrastructure Workflow.
This Python Pipeline should only be run following the successful completion of the RMB Pastoral Infrastructure
Transition Pipeline, and only once the RMB Manager has verified the data. This pipeline creates a backup prior
to and following the data update, appends the new data into the corporate data set, archives the existing data
in the corporate data set to the archive data set, overwrites and overwrites the snapshot dataset based on the
current date.



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

# import modules
from __future__ import print_function, division
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


def mk_dir(path_):
    if not os.path.isdir(path_):
        os.mkdir(path_)


def delete_pdf(path_):
    for file_ in glob(os.path.join(path_, "*.pdf")):
        print("delete: ", file_)
        try:
            os.remove(file_)
        except:
            print("!"*50)
            print("Error while deleting file ", file_)
            


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
    print("initiate COPY TO WORKING")
    print("source: ", source_dir)
    print("export_dir", export_dir)
    print('Overwriting pastoral infrastructure shapefiles is in progress........')

    type_list = ["Points", "Lines", "Polys_Paddocks", "Polys_Other"]

    source_lib_corp = os.path.join(source_dir, "Lib_Corporate", "Data", "ESRI")
    export_lib_corp = os.path.join(export_dir, "Lib_Corporate", "Data", "ESRI")

    source_snap_corp = os.path.join(source_dir, "Annual_Snapshot")
    export_snap_corp = os.path.join(export_dir, "Annual_Snapshot")

    source_archive = os.path.join(source_dir, "Archive", "Data", "ESRI")
    export_archive = os.path.join(export_dir, "Archive", "Data", "ESRI")

    source_pdf = os.path.join(source_dir, "Lib_Corporate", "Products", "Maps")
    export_pdf = os.path.join(export_dir, "Lib_Corporate", "Products", "Maps")

    source_arc_pdf = os.path.join(source_dir, "Archive", "Maps", "A0_MonSites")
    export_arc_pdf = os.path.join(export_dir, "Archive", "Maps", "A0_MonSites")

    source_other_pdf = os.path.join(source_dir, "Archive", "Maps", "All_Other_Maps")
    export_other_pdf = os.path.join(export_dir, "Archive", "Maps", "All_Other_Maps")

    for file_name in type_list:

        # --------------------------------------------- Lib Corporate --------------------------------------------------
        print("-"*50)
        print("Corporate Shapefiles")

        input_path = os.path.join(source_lib_corp, "Pastoral_Infra_{0}.shp".format(file_name))
        if os.path.isfile(input_path):
            print("- Located: ", input_path)

            # call the get_schema function to export the required shapefile schema
            corp_schema = get_schema_fn(input_path)
            gdf = gpd.read_file(input_path, driver="ESRI Shapefile")
            export_path = os.path.join(export_lib_corp, "Pastoral_Infra_{0}.shp".format(file_name))

            gdf.to_file(export_path, driver="ESRI Shapefile", schema=corp_schema)
            print("file copied to: ", export_path)

        else:
            print("Error - file not located: ", input_path)

        # --------------------------------------------- Lib Corporate Snapshot --------------------------------------------------
        print("-"*50)
        print("Snapshot Shapefiles")

        snap_list = next(os.walk(source_snap_corp))[1]

        for year_ in snap_list:
            print("year located: ", year_)
            year_snap = os.path.join(source_snap_corp, year_)

            input_path = os.path.join(year_snap, "{0}_Pastoral_Infra_{1}.shp".format(str(year_), file_name))
            if os.path.isfile(input_path):
                print("- Located: ", input_path)

                # call the get_schema function to export the required shapefile schema
                corp_schema = get_schema_fn(input_path)
                gdf = gpd.read_file(input_path, driver="ESRI Shapefile")

                export_year_path = os.path.join(export_snap_corp, str(year_))
                print("export_year_path: ", export_year_path)

                mk_dir(export_year_path)
        
                export_path = os.path.join(export_snap_corp, str(year_), "{0}_Pastoral_Infra_{1}.shp".format(str(year_), file_name))

                gdf.to_file(export_path, driver="ESRI Shapefile", schema=corp_schema)
                print("file copied to: ", export_path)

            else:
                print("Error - file not located: ", input_path)


        # ------------------------------------------------ Archive -----------------------------------------------------

        print("-"*50)
        print("Archived Shapefiles")
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

    # ------------------------------------------------Corporate --------------------------------------------------------
    print("-"*50)
    print("Working on PDFs")
    # call the delete pdf function to delete all pdfs within the export dir
    delete_pdf(export_pdf)

    for pdf_path in glob(os.path.join(source_pdf, "*.pdf")):

        head_tail = os.path.split(pdf_path)
        export_pdf_path = os.path.join(export_pdf, head_tail[1])

        try:
            shutil.copy(pdf_path, export_pdf_path)
            print("copied to: ", export_pdf_path)

        # If there is any permission issue
        except PermissionError:
            print("ERROR ---- "*10)
            print("Permission denied: ", export_pdf)
            print("File open - this will need to be moved manually")

    # ------------------------------------------------- Archive --------------------------------------------------------
            
    # call the delete pdf function to delete all pdfs within the export dir
    delete_pdf(export_arc_pdf)

    for pdf_path in glob(os.path.join(source_arc_pdf, "*.pdf")):

        head_tail = os.path.split(pdf_path)
        export_pdf_path = os.path.join(export_arc_pdf, head_tail[1])

        try:
            shutil.copy(pdf_path, export_pdf_path)
            print("copied: ", export_pdf_path)

        # If there is any permission issue
        except PermissionError:
            print("ERROR ---- "*10)
            print("Permission denied: ", export_arc_pdf)
            print("File open - this will need to be moved manually")

    # ------------------------------------------------- Other ----------------------------------------------------------

    print("-" * 50)
    print("Working on other PDFs")
    # call the delete pdf function to delete all pdfs within the export dir
    delete_pdf(export_other_pdf)

    for pdf_path in glob(os.path.join(source_other_pdf, "*.pdf")):

        head_tail = os.path.split(pdf_path)
        export_pdf_path = os.path.join(export_other_pdf, head_tail[1])

        try:
            shutil.copy(pdf_path, export_pdf_path)
            print("copied to: ", export_pdf_path)

        # If there is any permission issue
        except PermissionError:
            print("ERROR ---- " * 10)
            print("Permission denied: ", export_other_pdf)
            print("File open - this will need to be moved manually")


if __name__ == '__main__':
    main_routine()