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
    """ Concatenate a list to a geo-dataframe and delete features.

    :param list_a: list object of open geo-dataframes.
    :return gdf: geo-dataframe object.
    """

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
    """ Create a new feature and update it with the current date.

    :param gdf: geo-dataframe.
    :return gdf: Updated geo-dataframe.
    """

    date = datetime.now()
    date_label = date.strftime("%Y-%m-%d")
    gdf['DATE_CURR'] = date_label

    return gdf


def schema_update_export_data_fn(gdf, path, corporate_infrastructure_file):
    """ Clean geo-dataframe update schema and export.

    :param gdf: geo-dataframe object.
    :param path: string object containing the export shapefile path.
    :param corporate_infrastructure_file: string object containing the path to the corporate infrastructure feature
    type shapefile.
    """
    if 'OBJECTID' in gdf.columns:
        del gdf['OBJECTID']
    if "DELETE" in gdf.columns:
        del gdf["DELETE"]

    gdf_orig = fiona.open(corporate_infrastructure_file)
    corp_schema = gdf_orig.schema

    gdf.to_file(path, schema=corp_schema)
    print(' - 1. Export data to MIGRATION DIRECTORY with updated schema')


def main_routine(correct_list, migration, feature_type, corporate_infrastructure):
    """

    """

    clean_gdf = clean_gdf_fn(correct_list)
    gdf = date_curr_update_fn(clean_gdf)

    if feature_type == "paddocks":
        updated_feature_type = "Polys_Paddocks"
    elif feature_type == "polygons":
        updated_feature_type = "Polys_Other"
    else:
        updated_feature_type = feature_type.title()

    corporate_infrastructure_file = "{0}\\Pastoral_Infra_{1}.shp".format(corporate_infrastructure, updated_feature_type)
    # print('corporate_infrastructure_file: ', corporate_infrastructure_file)

    path = "{0}\\Pastoral_Infra_{1}.shp".format(migration, updated_feature_type)
    check_path = os.path.exists(path)

    if check_path:
        print(' - 1. ', feature_type.title(), ' data EXISTS in the MIGRATION DIRECTORY.')
        orig_gdf = gpd.read_file(path, driver="ESRI Shapefile")

        new_prop_list = gdf['PROPERTY'].unique().tolist()
        list_a = []

        for orig_prop in orig_gdf['PROPERTY'].unique():
            if orig_prop in new_prop_list:
                print('prop is NOT the same - do not keep.')
                pass
            else:
                orig_prop_gdf = orig_gdf[orig_gdf['PROPERTY'] == orig_prop]
                list_a.append(orig_prop_gdf)

        if len(list_a) > 0:
            migration_gdf = pd.concat(list_a)
            migration_gdf.append(gdf)
        else:

            migration_gdf = gdf

        schema_update_export_data_fn(migration_gdf, path, corporate_infrastructure_file)

    else:
        print(' - No ', feature_type.title(), ' data exists in the migration directory.')
        schema_update_export_data_fn(gdf, migration, path, corporate_infrastructure_file)


if __name__ == '__main__':
    main_routine()
