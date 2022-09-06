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
"""

# import modules
from __future__ import print_function, division
import os
from datetime import datetime
import argparse
import shutil
import sys
import warnings
from glob import glob
import pandas as pd
import geopandas as gpd

warnings.filterwarnings("ignore")


def directory_path_fn(direc, year, feature_type):
    """ Create a path to all property sub-directories and return them as a list.

    :param path: string object containing the path to the Pastoral Districts directory.
    :return prop_list: list object containing the path to all property sub-directories.
    """

    year_dir = "{0}\\{1}".format(direc, str(year))
    check_dir = os.path.isdir(year_dir)
    if not check_dir:
        os.mkdir(year_dir)

    item_dir = "{0}\\for_migration\\{1}".format(year_dir, feature_type)
    check_dir = os.path.isdir(item_dir)
    if not check_dir:
        os.mkdir(item_dir)
        path_ = None

    else:
        path_ = item_dir

    return path_


def previous_update_folder_structure(feature_type, assets_dir, year):
    year_dir = (
        '{0}\\shapefile\\previous_uploads\\{1}'.format(assets_dir, year))
    check_dir = os.path.isdir(year_dir)
    if not check_dir:
        os.mkdir(year_dir)

    previous_dir = "{0}\\previous_uploads".format(year_dir, feature_type)
    check_dir = os.path.isdir(previous_dir)
    if not check_dir:
        os.mkdir(previous_dir)

    dir_list_dir = "{0}\\previous_uploads".format(previous_dir, feature_type)
    check_dir = os.path.isdir(previous_dir)
    if not check_dir:
        os.mkdir(dir_list_dir)

    return dir_list_dir


'''def compare_dataframes_fn(previous, input_data_gdf):
    """ Compare two dataframes for the same observations ignoring the index column and removing duplicates from the
    output geo-dataframe

    :param previous: geo-dataframe object containing previous upload data.
    :param input_data_gdf: geo-dataframe object containing the located data for upload.
    :return edited_data_fn: geo-dataframe containing the only new data.
    """
    # print('previous data: ', previous.shape)
    # print('input_data_gdf: ', input_data_gdf)
    # check for the same observations in the new data as in the previous data, and remove them from the new data.
    input_data_gdf2 = input_data_gdf.merge(previous, how='outer', indicator='merge_')
    edited_data_df = input_data_gdf2[input_data_gdf2['merge_'] == 'left_only']
    edited_data_df['check'] = 'left'
    # print('edited_data_df: ', edited_data_df.shape)
    edited_data_df.drop(columns=['merge_'], inplace=True)
    print('edited_data_df: ', edited_data_df.shape)

    return edited_data_df'''


def compare_dataframes_fn(previous, input_data_gdf):
    """ Compare two dataframes for the same observations ignoring the index column and removing duplicates from the
    output geo-dataframe

    :param previous: geo-dataframe object containing previous upload data.
    :param input_data_gdf: geo-dataframe object containing the located data for upload.
    :return edited_data_fn: geo-dataframe containing the only new data.
    """
    print('previous data: ', previous.shape)
    print('input_data_gdf: ', input_data_gdf.shape)
    print('columns_' * 20)
    print('previous data: ', list(previous.columns))
    print('input_data_gdf: ', list(input_data_gdf.columns))

    previous.drop(columns=['DATE_CURR_2'], inplace=True)
    input_data_gdf.drop(columns=['DATE_CURR_2'], inplace=True)

    # check for the same observations in the new data as in the previous data, and remove them from the new data.
    previous.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\previous_line274.csv")
    input_data_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\input_data_line275.csv")
    input_data_gdf2 = input_data_gdf.merge(previous, how='outer', indicator='merge_',
                                           on=['FEATGROUP', 'FEATURE', 'LABEL', 'DATE_INSP', 'DISTRICT', 'PROPERTY',
                                               'PROP_TAG', 'SOURCE', 'CONFIDENCE', 'MAPDISPLAY', 'DELETE', 'STATUS',
                                               'geometry'])
    input_left_df = input_data_gdf2[input_data_gdf2['merge_'] == 'left_only']
    input_left_df['check'] = 'left'
    print('-' * 50)

    # edited_data_df.drop(columns=['merge_'], inplace=True)
    input_left_df.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\input_left_df_292.csv")
    # ------------------------------------------------------------------------------------------------------------------
    previous_left_df2 = previous.merge(input_data_gdf, how='outer', indicator='merge_',
                                       on=['FEATGROUP', 'FEATURE', 'LABEL', 'DATE_INSP', 'DISTRICT', 'PROPERTY',
                                           'PROP_TAG', 'SOURCE', 'CONFIDENCE', 'MAPDISPLAY', 'DELETE', 'STATUS',
                                           'geometry'])

    previous_left_df = previous_left_df2[previous_left_df2['merge_'] == 'left_only']
    previous_left_df['check'] = 'left'
    print('-' * 50)
    # edited_data_df.drop(columns=['merge_'], inplace=True)
    previous_left_df.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\previous_left_df2_line_300.csv")

    return input_left_df, previous_left_df


def previous_uploads_fn(input_data_gdf, migration, feature_type, assets_dir, year):
    """

    :param export_dir:
    :param direc:
    :param year:
    :param list_a: list object containing open dataframes.
    :param feature_type: string object containing the feature name.
    :return df_concat: geopandas dataframe object:
    """

    print('n' * 50)
    print('Previous uploads_fn: ', previous_uploads_fn)

    previous_dir_search = (
        '{0}\\shapefile\\previous_uploads\\{1}\\{2}\\previous_uploads_{2}_gd94.shp'.format(assets_dir, year,
                                                                                           feature_type))
    dir_list_dir = previous_update_folder_structure(feature_type, assets_dir, year)
    print('dir_list_dir: ', dir_list_dir)

    shapefile = "{0}\\prev_upload_{1}.gda94".format(dir_list_dir, feature_type)
    # Create a Boolean variable as to whether the file exists or not
    check_file = os.path.exists(shapefile)

    if check_file:
        print("previous data exists")
        previous_data_gdf = gpd.read_file(shapefile)
        edited_data_df = compare_dataframes_fn(previous_data_gdf, input_data_gdf)

    else:
        print('No previous data......')
        edited_data_df = input_data_gdf

    return edited_data_df


def migration_export_shapefile_fn(edited_gdf, migration, feature_type):
    print(':' * 50)
    print('export directory function....................')
    feature_type_dict = {"lines": "Pastoral_Infra_Lines", "points": "Pastoral_Infra_Points",
                         "polygon": "Pastoral_Infra_Polys_other", "paddocks": "Pastoral_Infra_Polys_Paddocks"}

    migration_dir = "{0}\\{1}".format(migration, feature_type)
    print('Migration directory: ', migration_dir)
    check_dir = os.path.isdir(migration_dir)
    if not check_dir:
        os.mkdir(migration_dir)

    edited_gdf = edited_gdf.replace(['Not Recorded'], "")
    shapefile = "{0}\\{1}.shp".format(migration_dir, feature_type_dict[feature_type])
    # Create a Boolean variable as to whether the file exists or not

    check_file = os.path.exists(shapefile)

    if check_file:
        print("data exists in migration folder")
        existing_data_gdf = gpd.read_file(shapefile)

        existing_data_gdf.append(edited_gdf)
        print(' - data appended: ', shapefile)

    else:
        print("data DOES NOT exist in migration folder")
        print(' - data transferred: ', shapefile)
        edited_gdf.to_file(shapefile, driver="ESRI Shapefile")


def check_crs_fn(gdf):
    gdf_crs = gdf.crs

    if gdf_crs == 4283:
        # print('correct crs')
        crs_check = True
    else:
        print('incorrect crs')
        gdf.to_file(r"Z:\Scratch\Zonal_Stats_Pipeline\Infrastructure_transition_DO_NOT_EDIT\faulty.shp")
        crs_check = False
    return crs_check


def return_to_prop_dir(trans_gdf, feature_type, pastoral_districts_path, year, date_label):
    """ Extract the file original location of the property specific geo-dataframe (Server_Upload)

    :param trans_gdf:
    :param feature_type:
    :param pastoral_districts_path:
    :param year:
    :return:
    """
    for prop in trans_gdf.PROPERTY.unique():
        print('prop: ', prop)
        property_gdf = trans_gdf[trans_gdf["PROPERTY"] == prop]
        prop_code = trans_gdf.loc[trans_gdf['PROPERTY'] == prop, 'PROP_TAG'].iloc[0]
        dist_ = trans_gdf.loc[trans_gdf["PROPERTY"] == prop, "DISTRICT"].iloc[0]

        dist = dist_.replace(' ', '_')

        if dist == 'Northern_Alice_Springs':
            district = 'Northern_Alice'
        elif dist == 'Southern_Alice_Springs':
            district = 'Southern_Alice'
        elif dist == 'Victoria_River':
            district = 'VRD'
        else:
            district = dist

        property_name = "{0}_{1}".format(prop_code, prop.replace(' ', '_').title())

        output_path = os.path.join(pastoral_districts_path, district, property_name, 'Infrastructure', 'Server_Upload',
                                   str(year), feature_type)
        output_file = "{0}\\uploaded_{1}_{2}.shp".format(output_path, feature_type, str(date_label))
        property_gdf.to_file(output_file)
        print('prop_output_' * 10)
        print(' - exported data to: ', output_file)


def property_export_shapefile_fn(prop_curr_test, pastoral_districts_path, feature_type, year):
    for prop_ in prop_curr_test.PROPERTY.unique():
        print(prop_)
        prop_filter = prop_curr_test[prop_curr_test['PROPERTY'] == prop_]
        dist_ = prop_curr_test.loc[prop_curr_test['PROPERTY'] == prop_, 'DISTRICT'].iloc[0]
        prop_code = prop_curr_test.loc[prop_curr_test['PROPERTY'] == prop_, 'PROP_TAG'].iloc[0]
        currency = prop_curr_test.loc[prop_curr_test['PROPERTY'] == prop_, 'DATE_CURR'].iloc[0]

        property_name = "{0}_{1}".format(prop_code, prop_.replace(' ', '_').title())
        dist = dist_.replace(' ', '_')

        if dist == 'Northern_Alice_Springs':
            district = 'Northern_Alice'
        elif dist == 'Southern_Alice_Springs':
            district = 'Southern_Alice'
        elif dist == 'Victoria_River':
            district = 'VRD'
        else:
            district = dist

        datetime_object = datetime.strptime(currency, "%m/%d/%Y").date()
        date_str = datetime_object.strftime("%Y%m%d")

        output_path = os.path.join(pastoral_districts_path, district, property_name, 'Infrastructure', 'Server_Upload',
                                   str(year), feature_type)

        prop_filter = prop_filter.replace(['Not Recorded'], "")
        prop_filter.to_file("{0}\\{1}_migration_{2}.shp".format(output_path, feature_type, date_str))


def filter_dataframe_fn(gdf):
    """ Filter dataframes on property and file into dist based on the STATUS column variables.
     -- Only VERIFIED  and AUTO_CORRECT can proceed.

    :param gdf: geo-dataframe object in transition directory.
    :return accurate_list: list object containing geo-dataframes filtered on property name and status.
    :return faulty_list: list object containing geo-dataframes filtered on property name and status.

    """
    property_list = gdf.PROPERTY.unique().tolist()
    faulty_list = []
    accurate_list = []
    for prop_ in property_list:
        print(prop_)
        prop_gdf = gdf[gdf['PROPERTY'] == prop_]

        # filter data in transition directory by status field keeping verified and auto correct.
        correct_gdf = prop_gdf[(prop_gdf['STATUS'] == 'VERIFIED') | (prop_gdf['STATUS'] == 'AUTO_CORRECT')]
        print(' - Data filtered by status (VERIFIED and AUTO CORRECT)')

        if len(correct_gdf.index) > 0:
            # update upload column with migration
            correct_gdf['UPLOAD'] = 'Migration'
            accurate_list.append(correct_gdf)

        # filter data in transition directory by status field keeping verified and auto correct.
        faulty_gdf = prop_gdf[(prop_gdf['STATUS'] != 'VERIFIED') | (prop_gdf['STATUS'] != 'AUTO_CORRECT')]
        print(' - Data filtered by status (VERIFIED and AUTO CORRECT)')

        if len(faulty_gdf.index) > 0:
            faulty_gdf['UPLOAD'] = 'Migration - FAILED'
            faulty_list.append(faulty_gdf)

    return accurate_list, faulty_list


def add_date_curr_2_fn(gdf):

    # convert string object to dateTime object
    gdf['DATE_CURR_2'] = pd.to_datetime(gdf['DATE_CURR'])

    return gdf


def add_migration_date_fn(trans_gdf):
    # extract date and update the date currency column (month / day / year)

    date = datetime.now()
    trans_gdf['MIGRATION'] = datetime.now()

    return trans_gdf


def update_date_curr_fn(trans_gdf):
    # extract date and update the date currency column (month / day / year)
    date = datetime.now()
    date_time_str = date.strftime("%d/%m/%Y - %H:%M:%S")
    date_curr = date.strftime("%Y-%m-%d")
    date_label = date.strftime("%Y%m%d")
    date_time_label = date.strftime("%Y%m%d_%H%M%S")
    # print(date_str)
    trans_gdf['DATE_CURR'] = date_curr
    trans_gdf['MIGRATION'] = date_time_str
    print(' - Currency date updated: ', date_curr)

    return trans_gdf, date_label, date_time_label


def date_label_fn():
    date = datetime.now()
    date_label = date.strftime("%Y%m%d")

    return date_label


def remove_columns_fn(trans_gdf):
    no_delete_gdf = trans_gdf[trans_gdf["DELETE"] == 0]
    # remove columns delete and status
    del no_delete_gdf["DELETE"]
    del no_delete_gdf["STATUS"]
    # print(list(no_delete_gdf.columns))

    final_gdf = no_delete_gdf.copy()
    del final_gdf["UPLOAD"]
    print(' - The following columns have been deleted:')
    print('    -- DELETE')
    print('    -- STATUS')
    print('    -- UPLOAD')

    return final_gdf


def previous_workflow_fn(curr_date_previous_df, filter_previous_df, prop_curr_test_df):
    if len(curr_date_previous_df.index) > 0:
        print(' - There are observations: ', len(filter_previous_df.index))
        # Previous data exists
        print('-' * 50)
        print('Checking that the data is new.... line 512')
        input_left_df, previous_left_df = compare_dataframes_fn(
            curr_date_previous_df,
            prop_curr_test_df)

        print('input_left_df: ', len(input_left_df.index),
              ' previous_left_df: ', len(previous_left_df.index))
        print('input_left_df: ', input_left_df)
        """print('The test data contains ', len(input_left_df.index),
              ' observations not in transition data.')
        print('len(input_left_df.index): ', len(input_left_df.index))

        print('The previous data contains: ', len(previous_left_df.index),
              ' observations not in test data.')
        print('len(previous_left_df.index): ', len(previous_left_df.index))"""

        if len(input_left_df.index) == 0 and len(previous_left_df.index) == 0:

            print(
                ' - Data already exists in the transition directory. line 587')
            # final_gdf = None
            """prop_curr_test_df['DELETE'] = 3
            add_to_previous_list.append(prop_curr_test_df)
            curr_date_previous_df['DELETE'] = 3
            delete_from_previous_list.append(curr_date_previous_df)"""
            print('-' * 50)
            # prop_curr_test_df['DELETE'] = 2
            migration_gdf = prop_curr_test_df
            # list_a.append(prop_curr_test_df)
            # print('gdf 505: ', prop_curr_test_df)
            # final_gdf_list.append(None)
            # print('-' * 50)

        elif len(input_left_df.index) > len(previous_left_df.index):
            print('The TEST data contains ',
                  str(len(input_left_df.index) - len(previous_left_df.index)),
                  ' more observations than the PREVIOUS data.')
            print('len(input_left_df.index): ', len(input_left_df.index))
            print('len(previous_left_df.index): ', len(previous_left_df.index))
            # delete_list.append(curr_date_previous_df)
            # delete_prev_list.append(prop_curr_test_df)
            """final_gdf_list.append(prop_curr_test_df)
            prop_curr_test_df['DELETE'] = 0
            add_to_previous_list.append(prop_curr_test_df)
            curr_date_previous_df['DELETE'] = 1"""
            """delete_from_previous_list.append(curr_date_previous_df)"""
            # prop_curr_test_df['DELETE'] = 0
            migration_gdf = prop_curr_test_df
            # list_a.append(prop_curr_test_df)
            # print('keep_gdf 524: ', prop_curr_test_df)
            # curr_date_previous_df['DELETE'] = 1
            # list_a.append(curr_date_previous_df)
            # print('delete_gdf 527: ', curr_date_previous_df)
            print('-' * 50)

        elif len(input_left_df.index) < len(previous_left_df.index):
            print('The PREVIOUS data contains ',
                  str(len(previous_left_df.index) - len(input_left_df.index)),
                  ' more observations than the TEST data.')
            print('len(input_left_df.index): ', len(input_left_df.index))
            print('len(previous_left_df.index): ', len(previous_left_df.index))
            # delete_list.append(curr_date_previous_df)
            # delete_prev_list.append(prop_curr_test_df)
            # final_gdf_list.append(prop_curr_test_df)
            """curr_date_previous_df['DELETE'] = 0
            add_to_previous_list.append(curr_date_previous_df)
            prop_curr_test_df['DELETE'] = 1
            delete_from_previous_list.append(prop_curr_test_df)"""

            # curr_date_previous_df['DELETE'] = 0
            # list_a.append(curr_date_previous_df)
            # print('keep_gdf 546: ', curr_date_previous_df)
            # prop_curr_test_df['DELETE'] = 1
            # todo I think this or above should be curr_date_previous_df
            migration_gdf = prop_curr_test_df
            # list_a.append(prop_curr_test_df)
            # print('delete_gdf 549: ', prop_curr_test_df)

            print('-' * 50)

    else:
        print('line 584 - else')
        # prop_curr_test_df['DELETE'] = 0
        migration_gdf = prop_curr_test_df
        # list_a.append(prop_curr_test_df)
        print('add_gdf 556: ', prop_curr_test_df)

    return migration_gdf


def upload_download_path_fn(prop_list):
    """ Create a path to to the Server Upload and Download sub-directories for each property and return them as a list.

    :param prop_list: list object containing the path to all property sub-directories.
    :return upload_list: list object containing the path to each properties Server_Upload sub-directory.
    :return download_list: list object containing the path to each properties Server_Upload sub-directory.
    """
    upload_list = []
    download_list = []
    for prop_path in prop_list:
        upload_path = os.path.join(prop_path, 'Infrastructure', 'Server_Upload')
        upload_list.append(upload_path)
        download_path = os.path.join(prop_path, 'Infrastructure', 'Server_Download')
        download_list.append(download_path)

    return upload_list, download_list


def property_path_fn(path):
    """ Create a path to all property sub-directories and return them as a list.

    :param path: string object containing the path to the Pastoral Districts directory.
    :return prop_list: list object containing the path to all property sub-directories.
    """
    # create a list of pastoral districts.
    dir_list = next(os.walk(path))[1]

    prop_list = []

    # loop through districts to get property name
    for district in dir_list:
        dist_path = os.path.join(path, district)

        property_dir = next(os.walk(dist_path))[1]

        # loop through the property names list
        for prop_name in property_dir:
            # join the path, district and property name to create a path to each property directory.
            prop_path = os.path.join(path, district, prop_name)
            # append all property paths to a list
            prop_list.append(prop_path)

    # print(prop_list)
    return prop_list



def main_routine(path, migration_dir, assets_dir, year, direc, export_dir, migration, asset_dir, directory_list,
                 transitory_dir_path_list, status_text):
    """

    """

    print('step2_2_search_folders.py INITIATED.')
    # loop through the directory list to extract a string object (feature_type) containing points, lines etc.

    migration_dir_list = []
    migration_list = []
    previous_list = []
    server_upload_list = []
    reject_list = []

    print('initiated....')
    # call the directory_path_fn function to create a path to all property sub-directories and return them as a list.
    prop_list = property_path_fn(path)
    print('prop_list: ', prop_list)
    print('-' * 50)
    # call the upload_download_path_fn function to create a path to to the Server Upload and Download sub-directories
    # for each property and return them as a list.
    upload_list, download_list = upload_download_path_fn(prop_list)
    print('upload_list: ', upload_list)
    print('download_list: ', download_list)

    for feature_type in directory_list:
        print('-' * 50)
        print('searching for: ', feature_type)
        print('-' * 50)
        print('=' * 50)

        # call the directory_path_fn function to create a path to all property sub-directories and return them as a string.
        dir_string = directory_path_fn(migration_dir, year, feature_type)
        print('dir_string: ', dir_string)
        if dir_string:
            migration_dir_list.append(dir_string)
            # create the file path to the relevant shapefile
            shapefile = "{0}\\Pastoral_Infra_{1}.shp".format(dir_string, feature_type.title())
            # append the file path to a list so that all data can be deleted at the end of the script.
            # Create a Boolean variable as to whether the file exists or not
            check_file = os.path.exists(shapefile)
            print('shapefile: ', shapefile)

            if check_file:
                print(' - located')
                print('Processing.........')
                # file exists
                gdf = gpd.read_file(shapefile)
                gdf_copy = gdf.copy()

                #gdf_copy['date_time_curr'] = datetime.now()
                # print(gdf.crs)
                crs_check = check_crs_fn(gdf_copy)

                if crs_check:

                    # add MIGRATION DATE column containing the current date time
                    gdf_copy = add_date_curr_2_fn(gdf_copy)

                    """ Filter dataframes on property and file into dist based on the STATUS column variables.
                     -- Only VERIFIED  and AUTO_CORRECT can proceed."""

                    property_list = gdf.PROPERTY.unique().tolist()
                    faulty_list = []
                    accurate_list = []
                    for prop_ in property_list:
                        print(prop_)
                        prop_gdf = gdf[gdf['PROPERTY'] == prop_]

                        if len(prop_gdf.index) > 0:
                            print(' - Number of observations: ', len(prop_filter_test_df.index))

                            # for loop through filtered test dataframe based on currency date.
                            for currency in prop_filter_test_df.DATE_CURR_2.unique():
                                print('CURRENCY: ', currency)
                                # calculate + and - 15 days from today's date (currency date).
                                start_date, end_date = thirty_day_currency_range_fn(currency)

                                # determine the latest currency date.
                                date_list = []
                                for i in prop_gdf.DATE_CURR_2.unique():
                                    date_list.append(i)
                                    # todo check that this actually works by adding fake data to the shapefile
                                print(
                                    ' Checking for the latest property specific currency date......')
                                latest_date = (max(date_list))
                                print(" - Latest_date: ", latest_date)


                        # todo delete sys
                        print('goodbye...............')
                        import sys
                        sys.exit()


                        # filter data in transition directory by status field keeping verified and auto correct.
                        correct_gdf = prop_gdf[
                            (prop_gdf['STATUS'] == 'VERIFIED') | (prop_gdf['STATUS'] == 'AUTO_CORRECT')]
                        print(' - Data filtered by status (VERIFIED and AUTO CORRECT)')

                        if len(correct_gdf.index) > 0:
                            # update upload column with migration
                            correct_gdf['UPLOAD'] = 'Migration'
                            accurate_list.append(correct_gdf)

                        # filter data in transition directory by status field keeping verified and auto correct.
                        faulty_gdf = prop_gdf[
                            (prop_gdf['STATUS'] != 'VERIFIED') | (prop_gdf['STATUS'] != 'AUTO_CORRECT')]
                        print(' - Data filtered by status (VERIFIED and AUTO CORRECT)')

                        if len(faulty_gdf.index) > 0:
                            faulty_gdf['UPLOAD'] = 'Migration - FAILED'
                            faulty_list.append(faulty_gdf)

                    return accurate_list, faulty_list




                    print(' - Coordinate reference system is correct.')

                    trans_gdf = filter_dataframe_fn(gdf_copy)

                    trans_gdf, date_label, date_time_label = update_date_curr_fn(trans_gdf)
                    '''# filter data in transition directory by status field keeping verified and auto correct.
                    trans_gdf = gdf[(gdf['STATUS'] == 'VERIFIED') | (gdf['STATUS'] == 'AUTO_CORRECT')]
                    print(' - Data filtered by status (VERIFIED and AUTO CORRECT)')
                    # update upload column with migration
                    trans_gdf['UPLOAD'] = 'Migration'

                    # extract date and update the date currency column (month / day / year)
                    date = datetime.now()
                    date_str = date.strftime("%m/%d/%Y")
                    date_curr = date.strftime("%Y-%m-%d")
                    date_label = date.strftime("%Y%m%d")
                    # print(date_str)
                    trans_gdf['DATE_CURR'] = date_curr
                    print(' - Currency date updated: ', date_curr)'''

                    # reformat inspection date to (month / day / year)
                    # inspect_date = trans_gdf['DATE_INSP'].tolist()
                    # formatted_date_list = []
                    # for i in inspect_date:
                    #    datetime_object = datetime.strptime(i, "%Y-%m-%d").date()
                    #    date_str = datetime_object.strftime("%m/%d/%Y")
                    #    # print(date_str)
                    #    formatted_date_list.append(date_str)

                    # trans_gdf["DATE_INSP"] = formatted_date_list
                    # print(' - Inspection date reformatted: ', date_str)
                    print('=' * 20)
                    print('trans_gdf columns', list(trans_gdf.columns))
                    # create a path object to the server upload sub-directory to update the status and upload columns.

                    date_label = date_label_fn()
                    # call the return to prop function to export the shapefile back to the property upload folder
                    return_to_prop_dir(trans_gdf, feature_type, path, year, date_label)

                    print('trans_gdf columns', list(trans_gdf.columns))

                    # call the remove columns function to delete the DELETE, STATUS AND UPLOAD columns
                    final_gdf = remove_columns_fn(trans_gdf)
                    '''no_delete_gdf = trans_gdf[trans_gdf["DELETE"] == 0]
                    # remove columns delete and status
                    del no_delete_gdf["DELETE"]
                    del no_delete_gdf["STATUS"]
                    # print(list(no_delete_gdf.columns))

                    final_gdf = no_delete_gdf.copy()
                    del final_gdf["UPLOAD"]
                    print(' - The following columns have been deleted:')
                    print('    -- DELETE')
                    print('    -- STATUS')
                    print('    -- UPLOAD')'''

                    previous_dir_path = os.path.join(migration_dir, str(year), "previous_transfer", feature_type)
                    print('previous_dir_path: ', previous_dir_path)

                    # migration_overwrite_list = []

                    # --------------------------------------
                    # searching through the transfer directory for data to migrate.
                    if glob("{0}\\{1}".format(previous_dir_path, "previous_transfer_*_gda94.shp")):

                        print(':' * 50)
                        print('Data has been located in the transition folder......')
                        print('Searching for previously migrated data.....')

                        print('feature_type: ', feature_type)
                        # = []
                        for file in glob("{0}\\{1}".format(previous_dir_path, "previous_transfer_*_gda94.shp")):

                            # open as geo-dataframe
                            previous_gdf = gpd.read_file(file, geometry="geometry")
                            previous_gdf_exists = True
                            # convert geo-dataframe to a pandas df
                            previous_df = pd.DataFrame(previous_gdf)
                            # convert string object to dateTime object
                            previous_df['DATE_CURR_2'] = pd.to_datetime(previous_df['DATE_CURR'])
                            print('.... located.')

                            # convert geo-dataframe to a pandas df
                            test_df = pd.DataFrame(final_gdf)
                            # convert string object to dateTime object
                            test_df['DATE_CURR_2'] = pd.to_datetime(test_df['DATE_CURR'])

                            print('Filter the test and previous data:')
                            # for loop through test dataframe based on property name.
                            for prop_ in test_df.PROPERTY.unique():
                                print(' - Property name: ', prop_)
                                # filter dataframe based on property name.
                                filter_test_df = test_df[test_df['PROPERTY'] == prop_]

                                if len(filter_test_df.index) > 0:
                                    print('- Number of observations: ', len(filter_test_df.index))
                                    # for loop through filtered test dataframe based on currency date.
                                    for currency in filter_test_df.DATE_CURR_2.unique():
                                        print(' - Currency date: ', currency)
                                        print('Checking for dates between:')
                                        # create two variables start and end dates to use to filter previous data
                                        start_date = currency + pd.Timedelta(days=15)
                                        print(' - Start_date: ', start_date)
                                        end_date = currency - pd.Timedelta(days=15)
                                        print(' - End_date: ', end_date)

                                        # filter the property filtered test data by currency date.
                                        prop_curr_test_df = filter_test_df[filter_test_df['DATE_CURR_2'] == currency]
                                        filter_previous_df = previous_gdf[
                                            (previous_df['PROPERTY'] == prop_) & (
                                                    previous_df['DATE_CURR_2'] < start_date) &
                                            (previous_df['DATE_CURR_2'] > end_date)]
                                        print('Filtered previous length: ', len(filter_previous_df.index))
                                        print('=' * 50)
                                        if len(filter_previous_df.index) > 0:
                                            # determine the latest currency date.
                                            date_list = []
                                            for i in filter_previous_df.DATE_CURR_2.unique():
                                                date_list.append(i)
                                                # todo check that this actually works by adding fake data to the shapefile
                                            print(
                                                ' Checking for the latest property specific currency date of the previous data......')
                                            latest_date = (max(date_list))
                                            print(" - Latest_date: ", latest_date)

                                            # filter the dataframe by the latest recorded curr_date
                                            curr_date_previous_df = previous_gdf[
                                                previous_df['DATE_CURR_2'] == latest_date]

                                            migration_gdf = previous_workflow_fn(curr_date_previous_df,
                                                                                 filter_previous_df, prop_curr_test_df)
                                            '''if len(curr_date_previous_df.index) > 0:
                                                print(' - There are observations: ', len(filter_previous_df.index))
                                                # Previous data exists
                                                print('-' * 50)
                                                print('Checking that the data is new.... line 512')
                                                input_left_df, previous_left_df = compare_dataframes_fn(
                                                    curr_date_previous_df,
                                                    prop_curr_test_df)

                                                print('input_left_df: ', len(input_left_df.index),
                                                      ' previous_left_df: ', len(previous_left_df.index))
                                                print('input_left_df: ', input_left_df)
                                                """print('The test data contains ', len(input_left_df.index),
                                                      ' observations not in transition data.')
                                                print('len(input_left_df.index): ', len(input_left_df.index))

                                                print('The previous data contains: ', len(previous_left_df.index),
                                                      ' observations not in test data.')
                                                print('len(previous_left_df.index): ', len(previous_left_df.index))"""

                                                if len(input_left_df.index) == 0 and len(previous_left_df.index) == 0:

                                                    print(
                                                        ' - Data already exists in the transition directory. line 587')
                                                    # final_gdf = None
                                                    """prop_curr_test_df['DELETE'] = 3
                                                    add_to_previous_list.append(prop_curr_test_df)
                                                    curr_date_previous_df['DELETE'] = 3
                                                    delete_from_previous_list.append(curr_date_previous_df)"""
                                                    print('-' * 50)
                                                    # prop_curr_test_df['DELETE'] = 2
                                                    list_a.append(prop_curr_test_df)
                                                    # print('gdf 505: ', prop_curr_test_df)
                                                    # final_gdf_list.append(None)
                                                    # print('-' * 50)

                                                elif len(input_left_df.index) > len(previous_left_df.index):
                                                    print('The TEST data contains ',
                                                          str(len(input_left_df.index) - len(previous_left_df.index)),
                                                          ' more observations than the PREVIOUS data.')
                                                    print('len(input_left_df.index): ', len(input_left_df.index))
                                                    print('len(previous_left_df.index): ', len(previous_left_df.index))
                                                    # delete_list.append(curr_date_previous_df)
                                                    # delete_prev_list.append(prop_curr_test_df)
                                                    """final_gdf_list.append(prop_curr_test_df)
                                                    prop_curr_test_df['DELETE'] = 0
                                                    add_to_previous_list.append(prop_curr_test_df)
                                                    curr_date_previous_df['DELETE'] = 1"""
                                                    """delete_from_previous_list.append(curr_date_previous_df)"""
                                                    # prop_curr_test_df['DELETE'] = 0
                                                    list_a.append(prop_curr_test_df)
                                                    # print('keep_gdf 524: ', prop_curr_test_df)
                                                    # curr_date_previous_df['DELETE'] = 1
                                                    # list_a.append(curr_date_previous_df)
                                                    # print('delete_gdf 527: ', curr_date_previous_df)
                                                    print('-' * 50)

                                                elif len(input_left_df.index) < len(previous_left_df.index):
                                                    print('The PREVIOUS data contains ',
                                                          str(len(previous_left_df.index) - len(input_left_df.index)),
                                                          ' more observations than the TEST data.')
                                                    print('len(input_left_df.index): ', len(input_left_df.index))
                                                    print('len(previous_left_df.index): ', len(previous_left_df.index))
                                                    # delete_list.append(curr_date_previous_df)
                                                    # delete_prev_list.append(prop_curr_test_df)
                                                    # final_gdf_list.append(prop_curr_test_df)
                                                    """curr_date_previous_df['DELETE'] = 0
                                                    add_to_previous_list.append(curr_date_previous_df)
                                                    prop_curr_test_df['DELETE'] = 1
                                                    delete_from_previous_list.append(prop_curr_test_df)"""

                                                    # curr_date_previous_df['DELETE'] = 0
                                                    # list_a.append(curr_date_previous_df)
                                                    # print('keep_gdf 546: ', curr_date_previous_df)
                                                    # prop_curr_test_df['DELETE'] = 1
                                                    list_a.append(prop_curr_test_df)
                                                    # print('delete_gdf 549: ', prop_curr_test_df)

                                                    print('-' * 50)

                                            else:
                                                print('line 584 - else')
                                                #prop_curr_test_df['DELETE'] = 0
                                                list_a.append(prop_curr_test_df)
                                                print('add_gdf 556: ', prop_curr_test_df)'''

                                        else:
                                            # prop_curr_test_df['DELETE'] = 0
                                            migration_gdf = prop_curr_test_df
                                            # list_a.append(prop_curr_test_df)
                                            print('add_gdf 561: ', prop_curr_test_df)
                    else:
                        print('No previous transition data exists.....')
                        # final_gdf_list.append(gdf)
                        # gdf['DELETE'] = 0
                        migration_gdf = gdf
                        # list_a.append(gdf)

                else:
                    print('ERROR!!!!!!')
                    print(' - Incorrect crs.')
                    print(' - ', feature_type, ' will not be processed - data must be in GDA94 geographics.')

            else:
                # file does not exist
                print(feature_type, ' ..... NOT located')
                print('=' * 50)
        else:
            print(dir_string, ' NOT located')
            print('=' * 50)

        print('-' * 50)
        print('-' * 50)
        print(';' * 50)
        print('finished for loop - migration_gdf: ', migration_gdf)
        # print(len(list_a))

        '''df_list = []
        for i in list_a:
            if isinstance(i, gpd.GeoDataFrame):
                print('Convert to pd DataFrame')
                df = pd.DataFrame(i)
                df_list.append(df)
                # gdf_filter = i[i['DELETE']==0]
                # del gdf_filter['DELETE']
                # transition_export_gdf_fn(gdf_filter, migration_dir, feature_type, year)
                # print(' - uploaded to transition directory')
                # property_export_shapefile_fn(gdf_filter, pastoral_districts_path, feature_type, year)
                # print(' - uploaded to property directory')
                # previous_transfer_export_gdf_fn(gdf_filter, migration_dir, feature_type, year)

            elif isinstance(i, pd.DataFrame):
                df_list.append(i)
                # gdf = gpd.GeoDataFrame(i, geometry='geometry')
                # gdf_filter = gdf[gdf['DELETE']==0]
                # del gdf_filter['DELETE']
                # print('Convert to geo-dataframe:')
                # transition_export_gdf_fn(gdf, migration_dir, feature_type, year)
                # print('Export geo-dataframes:')
                # print(' - uploaded to transition directory')
                # property_export_shapefile_fn(gdf, pastoral_districts_path, feature_type, year)
                # print(' - uploaded to property directory')
                # previous_transfer_export_gdf_fn(gdf, migration_dir, feature_type, year)

            else:
                print("!!!!! - not considered a geo-dataframe")'''

        gdf = gpd.GeoDataFrame(migration_gdf, geometry='geometry')
        dir_list_dict = {"points": "Pastoral_Infra_Points",
                         "lines": "Pastoral_Infra_Lines",
                         "paddocks": "Pastoral_Infra_Paddocks",
                         "polygons": "Pastoral_Infra_Polygons"}

        dir_list_output = dir_list_dict[feature_type]
        print(']' * 50)
        print('dir_list_output: ', dir_list_output)

        migration_shapefile = (
            "{0}\\{1}\\{2}.shp".format(migration, feature_type, dir_list_output))

        print('migration shapefile: ', migration_shapefile)
        check_file = os.path.exists(migration_shapefile)

        if check_file:
            """dir_path = "{0}\\{1}\\previous_transfer\\{2}".format(migration_dir, year, feature_type)
            shutil.rmtree(dir_path)
            os.mkdir(dir_path)"""

        """print('df_list: ', df_list)
        df = pd.concat(df_list)
        df.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\Infrastructure_transition_DO_NOT_EDIT\test\df_migration_test.csv")
        print('+' * 50)"""

        # final_df = df[df['DELETE'] != 1]
        print(final_gdf.info())
        # output_gdf = gpd.GeoDataFrame(df, geometry='geometry')
        output_gdf = gdf
        if "DATE_CURR_2" in output_gdf.columns:
            del output_gdf["DATE_CURR_2"]
        if "DELETE" in output_gdf.columns:
            del output_gdf["DELETE"]

        print(output_gdf.info())
        output_gdf.to_file(migration_shapefile)
        print('=' * 50)
        print('migration_shapefile: ', migration_shapefile)
        print('=' * 50)

    for i in migration_dir_list:
        print(i)
        # todo add rmtree back
        # shutil.rmtree(i)


if __name__ == '__main__':
    main_routine()
