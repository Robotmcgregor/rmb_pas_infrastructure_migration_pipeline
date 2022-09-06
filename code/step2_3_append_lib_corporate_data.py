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
import warnings
import pandas as pd
import geopandas as gpd
from glob import glob
import fiona

warnings.filterwarnings("ignore")


def clean_gdf_fn(list_a):
    gdf = pd.concat(list_a)

    if "STATUS" in gdf.columns:
        del gdf["STATUS"]

    if "M_DATE" in gdf.columns:
        del gdf["M_DATE"]

    if "M_DATE_R" in gdf.columns:
        del gdf["M_DATE_R"]

    if "DATE_CURR_2" in gdf.columns:
        del gdf["DATE_CURR_2"]

    return gdf


def date_curr_update_fn(gdf):
    date = datetime.now()
    date_label = date.strftime("%Y-%m-%d")
    gdf['DATE_CURR'] = date_label

    return gdf


def export_data_fn(gdf, migration, updated_feature_type):
    path_ = "{0}\\Pastoral_Infra_{1}.shp".format(migration, updated_feature_type)
    gdf.to_file(path_, driver="ESRI Shapefile")


def schema_update_export_data_fn(gdf, corporate_infrastructure_file):
    '''if 'OBJECTID' not in gdf.columns:
        gdf.insert(0, "OBJECTID", gdf.index + 1)'''
    if 'OBJECTID' in gdf.columns:
        del gdf['OBJECTID']
    if "DELETE" in gdf.columns:
        del gdf["DELETE"]

    print('-' * 50)
    print('schema_update_export_data_fn STARTING')
    print('-' * 50)
    gdf_orig = fiona.open(corporate_infrastructure_file)
    corp_schema = gdf_orig.schema

    original_gdf = gpd.read_file(corporate_infrastructure_file, driver='ESRI Shapefile')
    print('gdf length: ', len(gdf.index))
    print('original_gdf length: ', len(original_gdf.index))

    dropped_df_list = []

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
                dropped_df = original_gdf.iloc[list_a, :]
                print('dropped_df: ', dropped_df)
                dropped_df_list.append(dropped_df)
                print('original_gdf length after drop: ', len(drop_gdf.index))

        else:
            print('not located')

    final_gdf = original_gdf.append(gdf)
    print('original_gdf AFTER APPEND length: ', len(final_gdf.index))
    final_gdf.to_file(corporate_infrastructure_file, schema=corp_schema)
    print(' - 1. Export data to MIGRATION DIRECTORY with updated schema')
    return dropped_df_list


def schema_update_append_data_fn(gdf, corporate_infrastructure_file):
    '''if 'OBJECTID' not in gdf.columns:
        gdf.insert(0, "OBJECTID", gdf.index + 1)'''
    if 'OBJECTID' in gdf.columns:
        del gdf['OBJECTID']
    # if "DELETE" in gdf.columns:
    #     del gdf["DELETE"]

    print('-' * 50)
    print('schema_update_append_data_fn STARTING')
    print('-' * 50)
    gdf_orig = fiona.open(corporate_infrastructure_file)
    corp_schema = gdf_orig.schema

    original_gdf = gpd.read_file(corporate_infrastructure_file, driver='ESRI Shapefile')
    print('new_data length: ', len(gdf.index))
    print('corporate library length: ', len(original_gdf.index))
    # print(corp_schema)
    archive_list = []
    new_prop_date_dict = {}

    # filter new data by property name
    for prop_ in gdf.PROPERTY.unique():
        print('property: ', prop_)

        # filter new data based on property name and extract the date_curr value
        prop_gdf = gdf[gdf['PROPERTY'] == prop_]

        if len(prop_gdf.index) == 1:
            print("data length is 1")
            if 2 in prop_gdf['DELETE'].unique().tolist():

                # todo up to here
                # check if same property name exists in existing data
                if prop_ in list(original_gdf.PROPERTY.unique()):
                    print('existing property data located in the original gdf......')
                    # filter original data by property if the same property name (new data) already exists in original data
                    prop_orig = original_gdf[original_gdf['PROPERTY'] == prop_]
                    archive_a = prop_orig.index.tolist()
                    archive_list.extend(archive_a)
                    # print('archive_a: ', archive_a)

        elif len(prop_gdf.index) > 1:

            date_curr_list = prop_gdf.DATE_CURR.unique().tolist()
            date_curr = date_curr_list[0]
            # print(date_curr)
            # append property name and currency date to a dictionary
            new_prop_date_dict[prop_] = date_curr

            # check if same property name exists in existing data
            if prop_ in list(original_gdf.PROPERTY.unique()):
                print('existing property data located in the original gdf......')
                # filter original data by property if the same property name (new data) already exists in original data
                prop_orig = original_gdf[original_gdf['PROPERTY'] == prop_]
                archive_a = prop_orig.index.tolist()
                archive_list.extend(archive_a)
                # print('archive_a: ', archive_a)

        else:
            print('not located')
            pass

    print('corporate library length: ', len(original_gdf.index))
    print('archived_list length: ', len(archive_list))
    # print('new_prop_date_dict: ', new_prop_date_dict)
    original_drop_gdf = original_gdf.drop(archive_list)

    print("gdf: ", gdf.shape)
    # append new data excluding DELETE==2 to the Corporate Library
    if "DELETE" in gdf.columns:
        gdf_removed_del2 = gdf[gdf['DELETE'].isin([0, 0., "0"])]
        del gdf_removed_del2["DELETE"]
    else:
        gdf_removed_del2 = gdf

    print("gdf_removed_del2: ", gdf_removed_del2.shape)

    final_transfer = original_drop_gdf.append(gdf_removed_del2)
    print('-' * 30)
    print('final_transfer length: ', len(final_transfer.index))
    print('-' * 50)

    if len(final_transfer.index) > 0:
        print('Final corporate library length: ', len(final_transfer.index))
        print('=' * 70)
        final_transfer.to_file(corporate_infrastructure_file, schema=corp_schema)

    else:
        print('ERRORRRRRRRRRRRRR')

    return archive_list, new_prop_date_dict, original_gdf


def schema_update_archive_data_fn(archive_list, original_gdf, new_prop_date_dict, archive_infrastructure_file,
                                  corporate_infrastructure_file):
    print('-' * 50)
    print('schema_update_archive_data_fn STARTING')
    print('-' * 50)

    gdf_archive = fiona.open(archive_infrastructure_file)
    archive_schema = gdf_archive.schema

    # print('archive_list: ', archive_list)
    print("corporate_infrastructure_file: ", corporate_infrastructure_file)

    original_gdf = gpd.read_file(corporate_infrastructure_file, driver='ESRI Shapefile')
    archive_gdf = gpd.read_file(archive_infrastructure_file, driver='ESRI Shapefile')
    # print('archive_gdf length: ', len(archive_gdf.index))

    new_property_data = []
    existing_property_data = []
    # loop through properties and currency date
    for key in new_prop_date_dict:
        print(key)
        # filter archived data by property name.
        prop_filter = archive_gdf[archive_gdf["PROPERTY"] == key]
        if len(prop_filter.index) > 0:
            # property data exists within the archived data set.
            value = new_prop_date_dict.get(key)

            # filter property archived data by currency date
            prop_date_filter = prop_filter[prop_filter["DATE_CURR"] == value]
            print("prop_date_filter length: ", len(prop_date_filter.index))
            if len(prop_date_filter.index) > 0:
                # property data with the same currency date already exists within the archived dataset.
                # extract the index values so that they can be dropped and replaced.
                archive_list = prop_date_filter.index.tolist()
                existing_property_data.extend(archive_list)

            else:
                # property data exists, but with a different currency date - no action required
                pass

        else:
            # no property data exists in the original dataset.
            print(key, " data not already located in archive dataset.")
            orig_prop = original_gdf[original_gdf["PROPERTY"] == key]
            new_property_data.append(orig_prop)

    import sys
    sys.exit()

    print("archive gdf length: ", archive_gdf.shape)
    if len(existing_property_data) > 0:
        archive_gdf.drop(existing_property_data, axis="index", inplace=True)
        print("archived data dropped")

    # else:
    #     archive_dropped_gdf = archive_gdf

    print("archive after dropped - gdf length: ", archive_gdf.shape)

    if len(new_property_data) > 0:
        concat_new = pd.concat(new_property_data)

        print(type(concat_new))
        updated_archive_gdf = archive_gdf.append(concat_new)
        print("archive_dropped_gdf length: ", archive_gdf.shape)
        print("Concat_new length: ", concat_new.shape)
        print("updated_archive_gdf length: ", updated_archive_gdf.shape)

    else:
        updated_archive_gdf = archive_gdf

    updated_archive_gdf.to_file(archive_infrastructure_file, schema=archive_schema)


def name_curr_date_dict_fn(gdf):
    """

    :param gdf: input geo-datafrmae
    :return: dictionary object containing property name and currency date of the new data.
    """
    prop_date_dic = {}

    # filter new data by property name
    for prop_ in gdf.PROPERTY.unique():
        # filter new data based on property name and extract the date_curr value
        prop_gdf = gdf[gdf['PROPERTY'] == prop_]

        for date_ in prop_gdf.DATE_CURR.unique():
            prop_date_dic[prop_] = date_

    return prop_date_dic


def get_schema_fn(path_):
    """
    :param path_: string object containing the path to a shapefile
    :return: dictionary object containing the schema of the passed shapefile
    """
    gdf_orig = fiona.open(path_)
    schema = gdf_orig.schema

    return schema


def append_corporate_data_fn(gdf, corporate_infrastructure_file, prop_date_dic):
    if 'OBJECTID' in gdf.columns:
        del gdf['OBJECTID']

    print('-' * 50)
    print(' - appending corporate dataset')
    print('-' * 50)

    to_archive_list = []
    orig_drop_list = []
    # delete_two_prop = []

    # call the get_schema function to export the required shapefile schema
    corp_schema = get_schema_fn(corporate_infrastructure_file)

    # read in located shapefile paths as dataframes
    corp_gdf = gpd.read_file(corporate_infrastructure_file, driver='ESRI Shapefile')
    print('new_data length: ', len(gdf.index))
    print('corporate library length: ', len(corp_gdf.index))

    for key in prop_date_dic:
        print("working on: ", key)
        # filter lib corporate data by property
        prop_orig = corp_gdf[corp_gdf['PROPERTY'] == key]

        if len(prop_orig.index) > 0:
            # property data located within lib corporate data set.
            to_archive_list.append(prop_orig)

            archive_list = prop_orig.index.tolist()
            orig_drop_list.extend(archive_list)

        else:
            # property data not located within lib corporate data set.
            pass

    return to_archive_list, orig_drop_list, corp_schema, corp_gdf


def append_archive_data_fn(archive_infrastructure_file, prop_date_dic):
    print('-' * 50)
    print('append_archive_data_fn STARTING')
    print('-' * 50)

    arch_drop_list = []

    # call the get_schema function to export the required shapefile schema
    arch_schema = get_schema_fn(archive_infrastructure_file)

    # print('archive_list: ', archive_list)
    print("archive_infrastructure_file: ", archive_infrastructure_file)

    # create a geo-dataframe from the archive dataset
    archive_gdf = gpd.read_file(archive_infrastructure_file, driver='ESRI Shapefile')

    # loop through properties and currency date
    for key in prop_date_dic:
        print("working on property: ", key)
        # filter archived data by property name.
        prop_filter = archive_gdf[archive_gdf["PROPERTY"] == key]
        if len(prop_filter.index) > 0:
            # property data exists within the archived data set.
            value = prop_date_dic.get(key)

            # filter property archived data by currency date
            prop_date_filter = prop_filter[prop_filter["DATE_CURR"] == value]
            print("prop_date_filter length: ", len(prop_date_filter.index))
            if len(prop_date_filter.index) > 0:
                # property name with the same currency date already exists within the archive dataset
                archive_list = prop_date_filter.index.tolist()
                arch_drop_list.extend(archive_list)

            else:
                # property data with a different currency date exists within the archived dataset.
                pass

        else:
            # property data does not exist within the archived dataset.
            pass

    return arch_drop_list, arch_schema, archive_gdf


def delete_two_fn(prop_date_dic, gdf):
    """ Search for delete = 2 data at the property level, if located and no delete = 0 data exists, the property name
    is removed form the prop_date_dic dictionary.

    :param prop_date_dic: dictionary object containing property name and currency date information.
    :param gdf: geo-dataframe object containing all data located in the For Migration data set.
    :return: prop_date_dic: updated dictionary object containing property name and currency date information.
    """
    corp_property_append_list = []
    print("Checking if delete == 2 exists in dataset")
    print(prop_date_dic)
    # check if delete == 2 data exists
    for prop in gdf.PROPERTY.unique():
        print("Working on: ", prop)

        # filter dataset by property name
        prop_new = gdf[gdf["PROPERTY"] == prop]
        # create a list of all delete column values
        delete_list = prop_new.DELETE.unique().tolist()
        print("Delete list: ", delete_list)
        # check if 2 exists within the delete list
        if any(a in delete_list for a in (2, 2., "2")):
            # filter dataset to only include 0
            prop_new_keep = prop_new[prop_new['DELETE'].isin([0, 0., "0"])]
            if len(prop_new_keep.index) > 0:
                # data set contains observations to be kept.
                corp_property_append_list.append(prop_new_keep)

            else:
                # dataset does not contain data to be kept.
                # del prop_date_dic[prop]
                pass

        else:
            # delete = 2 data was not located
            corp_property_append_list.append(prop_new)

    return corp_property_append_list


def main_routine(correct_list, feature_type, corporate_infrastructure, archive_infrastructure):
    """

    """
    print('='*50)
    print("Working on ", feature_type)
    clean_gdf = clean_gdf_fn(correct_list)
    gdf = date_curr_update_fn(clean_gdf)

    if feature_type == "Paddocks":
        updated_feature_type = "Polys_Paddocks"
    elif feature_type == "Polygons":
        updated_feature_type = "Polys_Other"
    else:
        updated_feature_type = feature_type.title()

    corporate_infrastructure_file = os.path.join(corporate_infrastructure, "Pastoral_Infra_{0}.shp".format(
        updated_feature_type))
    print('corporate_infrastructure_file: ', corporate_infrastructure_file)

    corp_check_path = os.path.exists(corporate_infrastructure_file)
    print('corp_check_path: ', corp_check_path)
    archive_infrastructure_file = os.path.join(archive_infrastructure, "Pastoral_Infra_{0}.shp".format(
        updated_feature_type))
    # print('corporate_infrastructure_file: ', corporate_infrastructure_file)
    archive_check_path = os.path.exists(archive_infrastructure_file)
    print('archive_check_path: ', archive_check_path)

    # call the name_curr_date_function to extract the property name and currency date information.
    prop_date_dic = name_curr_date_dict_fn(gdf)
    print(prop_date_dic)

    # check if a property only contains delete 2 data
    corp_property_append_list = delete_two_fn(prop_date_dic, gdf)

    if corp_check_path:
        print(' - ', feature_type.title(), ' data EXISTS in the Lib Corporate dataset.')
        to_archive_list, orig_drop_list, corp_schema, corp_gdf = append_corporate_data_fn(
            gdf, corporate_infrastructure_file, prop_date_dic)

    else:
        print(' -- ERROR...... Lib Corporate data is missing please verify data exists and check your path')
        import sys
        sys.exit()

    # ------------------------------------------------------------------------------------------------------------------

    if archive_check_path:
        print(' - ', feature_type.title(), ' data EXISTS in the MIGRATION DIRECTORY.')
        arch_drop_list, arch_schema, archive_gdf = append_archive_data_fn(archive_infrastructure_file, prop_date_dic)

    else:
        print(' -- ERROR...... Archive data is missing please verify data exists and check your path')
        import sys
        sys.exit()

    # ----------------------------------------------- ARCHIVE ----------------------------------------------------------
    print("------------------ Archive -------------------------")
    # drop same prop and currency date data from archive dataset
    print("length of archive drop list: ", len(arch_drop_list))
    print("- archive_gdf: ", len(archive_gdf.index))
    rows = archive_gdf.index[arch_drop_list]
    archive_gdf.drop(rows, inplace=True)
    print("- archive_gdf after DROP: ", len(archive_gdf.index))

    # append selected lib corporate data to archives dataset.
    if len(to_archive_list) > 0:
        all_new_archive = pd.concat(to_archive_list)
        archive_final = archive_gdf.append(all_new_archive)
        print("- all_new_archive: ", len(all_new_archive.index))
    else:
        archive_final = archive_gdf

    print("- before append archive_gdf: ", len(archive_gdf.index))
    print("- after append archive_final: ", len(archive_final.index))

    if 'DELETE' in archive_final.columns:
        del archive_final['DELETE']

    archive_final.to_file(archive_infrastructure_file, driver="ESRI Shapefile", schema=arch_schema)

    # --------------------------------------------- LIB CORPORATE ------------------------------------------------------
    print("------------------ LIB Corporate -------------------------")
    print("length of orig_drop_list: ", len(orig_drop_list))
    print("- corp_gdf: ", len(corp_gdf.index))

    rows = corp_gdf.index[orig_drop_list]
    corp_gdf.drop(rows, inplace=True)
    print("- corp_gdf after DROP: ", len(corp_gdf.index))

    if len(corp_property_append_list) > 0:
        # append selected lib corporate data to archives dataset.
        all_new_corp = pd.concat(corp_property_append_list)
        corp_final = corp_gdf.append(all_new_corp)
        print("- all_new_corp: ", len(all_new_corp.index))
    else:
        corp_final = corp_gdf

    print("- before append corp_gdf: ", len(corp_gdf.index))
    print("- after append corp_final: ", len(corp_final.index))

    if 'DELETE' in corp_final.columns:
        del corp_final['DELETE']

    corp_final.to_file(corporate_infrastructure_file, driver="ESRI Shapefile", schema=corp_schema)

    # print('stop here. ')
    # import sys
    # sys.exit()
if __name__ == '__main__':
    main_routine()
