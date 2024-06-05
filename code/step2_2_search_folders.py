# !/usr/bin/env python

"""
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
Date: 2021
Email: robert.mcgregor@nt.gov.au
"""

# import modules
from __future__ import print_function, division
import os
import warnings
import pandas as pd
import geopandas as gpd
warnings.filterwarnings("ignore")


def directory_path_fn(direc, feature_type):
    """ Create a path to all directory sub-directories and return as a path.

    :param feature_type: string object containing a sub-directory name.
    :param direc: string object containing the directory path.
    :return path_: string object containing the path to final sub-directory.
    """

    item_dir = os.path.join(direc, "For_Migration", feature_type)
    check_dir = os.path.isdir(item_dir)
    if not check_dir:
        os.mkdir(item_dir)
        path_ = None

    else:
        path_ = item_dir

    return path_


def check_crs_fn(gdf):
    """ Check the coordinate reference system of the geo-dataframe; and re-project if required.

    :param gdf: geo-dataframe object.
    :return crs_check: variable object containing a boolean value.
    """
    gdf_crs = gdf.crs

    if gdf_crs == 4283:
        crs_check = True
    else:
        print(' -- Incorrect crs - converted to 4283')
        gdf.to_crs(epsg=4283)
        crs_check = True

    return crs_check


def add_date_curr_2_fn(gdf):
    """ Create a new column from the currency date convert string object to date time stamp.

    :param gdf: geo-dataframe object.
    :return gdf: Updated geo-dataframe object.
    """

    # convert string object to dateTime object
    gdf['DATE_CURR_2'] = pd.to_datetime(gdf['DATE_CURR'])

    return gdf


def work_flow_fn(migration_dir, feature_type):
    """ Manages the workflow of the feature type.

    :param migration_dir:
    :param year:
    :param feature_type:
    :return:
    """
    migration_dir_list = []
    failed_list = []
    correct_list = []
    # call the directory_path_fn function to create a path to all property sub-directories and return them as a string.
    dir_string = directory_path_fn(migration_dir, feature_type)
    # print('dir_string: ', dir_string)
    if dir_string:
        migration_dir_list.append(dir_string)
        # create the file path to the relevant shapefile
        shapefile = "{0}\\Pastoral_Infra_{1}.shp".format(dir_string, feature_type.lower())
        # append the file path to a list so that all data can be deleted at the end of the script.
        # Create a Boolean variable as to whether the file exists or not
        check_file = os.path.exists(shapefile)

        if check_file:
            print('Processing: ', feature_type)
            print('='*30)
            # file exists
            gdf = gpd.read_file(shapefile)
            gdf_copy = gdf.copy()

            # call the check_crs function to check or re-project geo-dataframe.
            crs_check = check_crs_fn(gdf_copy)

            if crs_check:
                # call the add date curr 2 function to create a date time feature from a string feature
                gdf_copy = add_date_curr_2_fn(gdf_copy)

                """ Filter dataframes on property and file into dist based on the STATUS column variables.
                 -- Only VERIFIED  and AUTO_CORRECT can proceed."""

                property_list = gdf_copy.PROPERTY.unique().tolist()

                for prop_ in property_list:
                    print('Working on property: ', prop_)
                    print('=' * 50)
                    prop_gdf = gdf_copy[gdf_copy['PROPERTY'] == prop_]

                    if len(prop_gdf.index) > 0:
                        prop_gdf['M_DATE_R'] = pd.to_datetime(prop_gdf["DATE_CURR"])
                        # determine the latest currency date.
                        # print('m_date_r')

                        date_list = []
                        for i in prop_gdf.M_DATE_R.unique():
                            # print(i)
                            date_list.append(i)

                        print(
                            ' Checking for the latest property specific currency date......')
                        latest_date = (max(date_list))
                        print(" - Latest_date: ", latest_date)

                        prop_date_gdf = prop_gdf[prop_gdf['M_DATE_R'] == latest_date]

                        if len(prop_date_gdf.index) > 0:

                            status_list = prop_date_gdf.STATUS.unique().tolist()
                            print("status list: ", status_list)
                            # filter data in transition directory by status field keeping verified and auto correct.
                            faulty_gdf = prop_date_gdf[
                                (~prop_date_gdf['STATUS'].str.contains('VERIFIED')) & (
                                    ~prop_date_gdf['STATUS'].str.contains('AUTO-CORRECT'))]

                            # if length is greater than 0 faulty data exists
                            if len(faulty_gdf.index) > 0:
                                print(" -- Contains UNVERIFIED data!!!")
                                failed_list.append(prop_date_gdf)
                            else:
                                correct_list.append(prop_date_gdf)
                                print(" -- Only contains VERIFIED or AUTO CORRECTED data")
                                pass

                        else:
                            print('ERROR - code not completed')
                            pass

                    else:
                        print('ERROR - code not completed')
                        pass

    return correct_list, failed_list


def main_routine(pastoral_districts_path, migration_dir, corporate_infrastructure, archive_infrastructure, year,
                 output_dir, faulty_output_dir):
    """
    This script searches through sub-directories and manages the feature type workflow.
    """

    print("Searching for Migration data .......")

    # ------------------------------------------------- POINTS ---------------------------------------------------------

    correct_points_list, faulty_points_list = work_flow_fn(migration_dir, 'points')

    if len(correct_points_list) > 0:
        import step2_3_append_lib_corporate_data
        step2_3_append_lib_corporate_data.main_routine(
            correct_points_list, 'points', corporate_infrastructure, archive_infrastructure)

        import step2_5_export_data_to_property
        step2_5_export_data_to_property.main_routine(correct_points_list, pastoral_districts_path, 'points', str(year))

    if len(faulty_points_list) > 0:
        print("ERROR -- FAULTY POINTS "*30)
        print("faulty_output_dir: ", faulty_output_dir)
        points_faulty_output_dir = os.path.join(faulty_points_list, "points")
        import step2_4_export_faulty_data
        step2_4_export_faulty_data.main_routine(faulty_points_list,  'points', output_dir, points_faulty_output_dir)

    # ------------------------------------------------- LINES ----------------------------------------------------------

    correct_lines_list, faulty_lines_list = work_flow_fn(migration_dir, 'lines')

    if len(correct_lines_list) > 0:
        import step2_3_append_lib_corporate_data
        step2_3_append_lib_corporate_data.main_routine(
            correct_lines_list, 'lines', corporate_infrastructure, archive_infrastructure)

        import step2_5_export_data_to_property
        step2_5_export_data_to_property.main_routine(correct_lines_list, pastoral_districts_path, 'lines', str(year))

    if len(faulty_lines_list) > 0:
        print("ERROR -- FAULTY LINES" * 30)
        print("faulty_output_dir: ", faulty_output_dir)
        lines_faulty_output_dir = os.path.join(faulty_points_list, "lines")
        import step2_4_export_faulty_data
        step2_4_export_faulty_data.main_routine(faulty_lines_list,  'lines', output_dir, lines_faulty_output_dir)

    # ------------------------------------------------- PADDOCKS -------------------------------------------------------

    correct_paddocks_list, faulty_paddocks_list = work_flow_fn(migration_dir, 'paddocks')

    if len(correct_paddocks_list) > 0:
        import step2_3_append_lib_corporate_data
        step2_3_append_lib_corporate_data.main_routine(correct_paddocks_list, 'paddocks', corporate_infrastructure,
                                                       archive_infrastructure)

        import step2_5_export_data_to_property
        step2_5_export_data_to_property.main_routine(correct_paddocks_list, pastoral_districts_path, 'paddocks', str(year))

    if len(faulty_paddocks_list) > 0:
        print("ERROR -- FAULTY PADDOCKS" * 30)
        print("faulty_output_dir: ", faulty_output_dir)
        paddocks_faulty_output_dir = os.path.join(faulty_points_list, "paddocks")
        import step2_4_export_faulty_data
        step2_4_export_faulty_data.main_routine(faulty_paddocks_list,  'paddocks', output_dir, paddocks_faulty_output_dir)

    # ----------------------------------------------- POLYGONS ---------------------------------------------------------

    correct_polygons_list, faulty_polygons_list = work_flow_fn(migration_dir, 'polygons')

    if len(correct_polygons_list) > 0:
        import step2_3_append_lib_corporate_data
        step2_3_append_lib_corporate_data.main_routine(correct_polygons_list, 'polygons', corporate_infrastructure,
                                                       archive_infrastructure)

        import step2_5_export_data_to_property
        step2_5_export_data_to_property.main_routine(correct_polygons_list, pastoral_districts_path, 'polygons', str(year))

    if len(faulty_polygons_list) > 0:
        print("ERROR -- FAULTY POLYGONS" * 30)
        print("faulty_output_dir: ", faulty_output_dir)
        polygons_faulty_output_dir = os.path.join(faulty_points_list, "polygons")
        import step2_4_export_faulty_data
        step2_4_export_faulty_data.main_routine(faulty_polygons_list,  'polygons', output_dir, polygonsfaulty_output_dir)


if __name__ == '__main__':
    main_routine()
