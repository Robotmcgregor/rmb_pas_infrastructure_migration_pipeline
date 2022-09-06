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
from datetime import timedelta
import numpy as np
import fiona

warnings.filterwarnings("ignore")


def convert_to_df_fn(previous_gdf, gdf):
    # convert geo-dataframe to a pandas df
    previous_df = pd.DataFrame(previous_gdf)

    # convert geo-dataframe to a pandas df
    test_df = pd.DataFrame(gdf)

    return test_df, previous_df


def thirty_day_currency_range_fn(currency):
    # create two variables start and end dates to use to filter previous data
    start_date = currency - pd.Timedelta(days=30)
    end_date = currency

    return start_date, end_date


def filter_data_currency_fn(start_date, end_date, prop_filter_test_df, prop_filter_transition_df, currency, prop_):
    """

    :param start_date: date time object containing the currency date - n of the test data.
    :param end_date: date time object containing the currency date + n of the test data.
    :param prop_filter_test_df: geo-dataframe containing the new data property  specific.
    :param prop_filter_transition_df: geo-dataframe containing the existing transition data.
    :param currency: date time object containing the currency date of the test data.
    :param prop_: string object containing the property name.
    :return prop_currency_test_df: geo-dataframe object containing the test data that has been filtered on property
    and currency date.
    :return prop_currency_filter_transition_df: geo-dataframe object containing all of the previous property specific
    data in the transition folder between the start and end dates - this data should be removed from the transition
    directory.
    """
    # filter the property filtered test data by currency date.
    '''print('+' * 50)
    print('filtering by: ', prop_)
    print(' currency: ', currency)
    print(' start_date: ', start_date)
    print(' end_date: ', end_date)
    print('+' * 50)'''

    prop_currency_test_df = prop_filter_test_df[prop_filter_test_df['TRAN_DATE'] == currency]

    #prop_currency_test_df.to_csv(
    #    r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload" + '\\' + prop_ + "_filtered_test_data.csv")

    prop_before_transition_df = prop_filter_transition_df[
        (prop_filter_transition_df['PROPERTY'] == prop_) & (prop_filter_transition_df['TRAN_DATE'] < start_date)]

    #prop_before_transition_df.to_csv(
    #    r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload" + '\\1_5_' + prop_ + "_prop_before_transition_df.csv")

    prop_after_transition_df = prop_filter_transition_df[
        (prop_filter_transition_df['PROPERTY'] == prop_) & (prop_filter_transition_df['TRAN_DATE'] >= start_date)]

    #prop_after_transition_df.to_csv(
    #    r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload" + '\\1_5_' + prop_ + "_prop_after_transition_df.csv")

    return prop_currency_test_df, prop_before_transition_df, prop_after_transition_df  # , keep_previous_df

def add_tran_date_fn(trans_gdf):

    trans_gdf['TRAN_DATE'] = pd.to_datetime(trans_gdf['DATE_CURR'])
    '''date_curr_list = trans_gdf.TRAN_DATE.tolist()
    print(date_curr_list)'''

    return trans_gdf


def filter_previous_data_latest_currency_date_fn(gdf):
    """

    """
    # determine the latest currency date.
    date_list = []
    for i in gdf.date_curr.unique():
        date_list.append(i)

    latest_date = (max(date_list))
    #print(" - date list: ", date_list)
    #print(" -- latest_date: ", latest_date)

    # filter the dataframe by the latest recorded curr_date
    latest_date_gdf = gdf[gdf['date_curr'] == latest_date]  # used to be previous_df
    other_date_gdf = gdf[gdf['date_curr'] != latest_date]
    '''start_date = latest_date - pd.Timedelta(days=30)
    print('start_date: ', start_date)
    other_date_gdf = gdf[(gdf['date_curr'] != latest_date) & (gdf['date_curr'] < start_date)]
    print('other_date_gdf : ', len(other_date_gdf.index))'''
    return latest_date_gdf, other_date_gdf


def remove_n_months_fn(to_archive_gdf):
    year_list = []
    month_list = []
    date_list = to_archive_gdf.date_curr.tolist()
    for i in date_list:
        year_ = i.year
        year_list.append(year_)

        month_ = i.month
        month_list.append(month_)

        print('year month: ', year_, month_)
    to_archive_gdf['year'] = year_list
    to_archive_gdf['month'] = month_list

    return to_archive_gdf

def schema_update_export_data_fn(gdf, file_path):


    if "date_curr" in gdf.columns:
        del gdf["date_curr"]

    gdf_orig = fiona.open(file_path)
    corp_schema = gdf_orig.schema

    original_gdf = gpd.read_file(file_path, driver='ESRI Shapefile')
    print('gdf length: ', len(gdf.index))
    print('original_gdf length: ', len(original_gdf.index))

    gdf.to_file(file_path, schema=corp_schema)


def schema_update_append_data_fn(gdf, file_path):

    if "date_curr" in gdf.columns:
        del gdf["date_curr"]

    gdf_orig = fiona.open(file_path)
    corp_schema = gdf_orig.schema

    archive_gdf = gpd.read_file(file_path, driver='ESRI Shapefile')
    print('gdf length: ', len(gdf.index))
    print('archive_gdf length: ', len(archive_gdf.index))
    final_gdf = archive_gdf.append(gdf)
    print('final_gdf length: ', len(final_gdf.index))
    final_gdf.to_file(file_path, schema=corp_schema)


def main_routine(feature_type, migration_dir, year, corporate_infrastructure, dropped_df_list):
    """ Move all data that is not the latest to the archived shapefile.

    """

    print('step 2_3_2'*20)
    print('dropped_df_list: ', dropped_df_list)
    archive_infrastructure = r'Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\Archive\Data\ESRI'

    if feature_type == "paddocks":
        updated_feature_type = "Polys_Paddocks"
    elif feature_type == "polygons":
        updated_feature_type = "Polys_Other"
    else:
        updated_feature_type = feature_type.title()

    corporate_infrastructure_file = "{0}\\Pastoral_Infra_{1}.shp".format(corporate_infrastructure, updated_feature_type)
    # print('corporate_infrastructure_file: ', corporate_infrastructure_file)

    orig_gdf = gpd.read_file(corporate_infrastructure_file, driver="ESRI Shapefile")
    orig_gdf['date_curr'] = pd.to_datetime(orig_gdf['DATE_CURR'])
    #print(orig_gdf.info())

    archive_infrastructure_file = "{0}\\Pastoral_Infra_{1}.shp".format(archive_infrastructure, updated_feature_type)
    # print('corporate_infrastructure_file: ', corporate_infrastructure_file)

    archive_gdf = gpd.read_file(archive_infrastructure_file, driver="ESRI Shapefile")
    if len(archive_gdf.index) >0:
        print('archive_gdf is NOT empty')
        archive_gdf['date_curr'] = pd.to_datetime(orig_gdf['DATE_CURR'])
        archive_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\archive_gdf.csv")

        for df in dropped_df_list:
            if len(df.index) > 0:
                df['date_curr'] = pd.to_datetime(orig_gdf['DATE_CURR'])
                print(list(df))
                df_date_list = df.date_curr.unique().tolist()
                print(df_date_list)

                for date in df_date_list:
                    archive_orig_date = archive_gdf[archive_gdf['DATE_CURR'] == date]
                    list_a = archive_orig_date.index.tolist()
                    print('prop_orig_date.index: ', list_a)
                    print('archive_gdf length before drop: ', len(archive_gdf.index))
                    archive_drop_gdf = archive_gdf.drop(list_a)
                    print('archive_gdf length after drop: ', len(archive_drop_gdf.index))

    else:
        print('archive_gdf is empty')
        dropped_final_df = pd.concat(dropped_df_list)
        print('dropped_final_df length before append: ', len(dropped_final_df.index))
        schema_update_export_data_fn(dropped_final_df, corporate_infrastructure_file)
        #dropped_final_df.to_file(corporate_infrastructure_file, schema=corp_schema)
        '''print(type(dropped_final_df))
        for df in dropped_df_list:
            print('archive length: ', len(archive_gdf.index))
            print('df length: ', len(df.index))
            print('-'*50)

            archive_gdf = archive_gdf.append(df)'''

        '''print('original_gdf AFTER APPEND length: ', len(final_gdf.index))
        final_gdf.to_file(corporate_infrastructure_file, schema=corp_schema)
        print(' - 1. Export data to MIGRATION DIRECTORY with updated schema')'''


        '''for df in dropped_df_list:
            if len(df.index) > 0:
                df['date_curr'] = pd.to_datetime(orig_gdf['DATE_CURR'])
                print(list(df))
                df_date_list = df.date_curr.unique().tolist()
                print(df_date_list)
    
                for date in df_date_list:
                    archive_orig_date = archive_gdf[archive_gdf['DATE_CURR'] == date]
                    list_a = archive_orig_date.index.tolist()
                    print('prop_orig_date.index: ', list_a)
                    print('archive_gdf length before drop: ', len(archive_gdf.index))
                    archive_drop_gdf = archive_gdf.drop(list_a)
                    print('archive_gdf length after drop: ', len(archive_drop_gdf.index))'''


    import sys
    sys.exit()
    '''latest_date_list = []
    other_date_list = []
    for prop in orig_gdf.PROPERTY.unique():
        #print('filtered by: ', prop)
        prop_orig_gdf = orig_gdf[orig_gdf['PROPERTY'] == prop]
        # call the filter_previous_data_latest_currency_date_fn function to filter dataframes at a property level with
        # the latest curr_date.

        latest_date_gdf, other_date_gdf = filter_previous_data_latest_currency_date_fn(prop_orig_gdf)
        #print('latest_date_gdf.index: ', len(latest_date_gdf.index))
        #print('other_date_gdf.index: ', len(other_date_gdf.index))
        if len(latest_date_gdf.index) > 0:
            latest_date_list.append(latest_date_gdf)
        if len(other_date_gdf.index) > 0:
            print(prop, ' has more than one date')
            print('latest_date_gdf.index: ', len(latest_date_gdf.index))
            print('other_date_gdf.index: ', len(other_date_gdf.index))
            other_date_list.append(other_date_gdf)

    print('=' * 50)
    if len(latest_date_list) > 0:
        lib_dir_keep_gdf = pd.concat(latest_date_list)
        if len(lib_dir_keep_gdf.index) > 0:
            print('before drop duplicates: ', len(lib_dir_keep_gdf.index))
            no_duplicates_gdf = lib_dir_keep_gdf.drop_duplicates() #subset=['FEATGROUP', 'FEATURE', 'LABEL', 'DATE_INSP', 'DATE_CURR', 'DISTRICT', 'PROPERTY', 'PROP_TAG', 'SOURCE', 'CONFIDENCE', 'LENGTH_M', 'MAPDISPLAY', 'geometry', 'date_curr'])
            print('after drop duplicates: ', len(no_duplicates_gdf.index))
            schema_update_export_data_fn(no_duplicates_gdf, corporate_infrastructure_file)
            print(' - 3. Final Lib Corporate output with updated schema')

    if len(other_date_list) > 0:
        to_archive_gdf = pd.concat(other_date_list)
        if len(to_archive_gdf.index) > 0:
            # to_archive_gdf2 = remove_n_months_fn(to_archive_gdf)
            schema_update_append_data_fn(to_archive_gdf, archive_infrastructure_file)
            print(' - 4. Final Archive output with updated schema')
    else:
        print(' - 4. No data to be Archived')'''

    '''# todo add this section to the archive script
    for prop_ in gdf.PROPERTY.unique():
        print(prop_)
        # filter new data based on property name
        prop_gdf = gdf[gdf['PROPERTY'] == prop_]
        # create a list of currency date records per property name - new data
        date_curr_list = prop_gdf.DATE_CURR.unique().tolist()

        if prop_ in list(original_gdf.PROPERTY.unique()):
            print('located')
            # filter original data by property if the same property name (new data) already exists in original data
            prop_orig = original_gdf[original_gdf['PROPERTY'] == prop_]
            for date in date_curr_list:
                prop_orig_date = prop_orig[prop_orig['DATE_CURR'] == date]
                list_a = prop_orig_date.index.tolist()
                print('prop_orig_date.index: ', list_a)
                print('original_gdf length before drop: ', len(original_gdf.index))
                drop_gdf = original_gdf.drop(list_a)
                print('original_gdf length after drop: ', len(drop_gdf.index))

        else:
            print('not located')

    final_gdf = original_gdf.append(gdf)
    print('original_gdf AFTER APPEND length: ', len(final_gdf.index))
    final_gdf.to_file(corporate_infrastructure_file, schema=corp_schema)
    print(' - 1. Export data to MIGRATION DIRECTORY with updated schema')'''











if __name__ == '__main__':
    main_routine()
