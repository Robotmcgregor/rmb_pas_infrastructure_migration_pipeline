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

warnings.filterwarnings("ignore")


def clean_gdf_fn(list_a):
    """ Clean features from geo-dataframe.

    :param list_a: list object containing open geo-dataframes.
    :return gdf: geo-dataframe that has been processed bu the function
    """
    gdf = pd.concat(list_a)

    if "M_DATE" in gdf.columns:
        del gdf["M_DATE"]

    if "M_DATE_R" in gdf.columns:
        del gdf["M_DATE_R"]

    if "DATE_CURR_2" in gdf.columns:
        del gdf["DATE_CURR_2"]

    if 'DELETE' in gdf.columns:
        del gdf['DELETE']

    return gdf


def property_export_shapefile_fn(prop_curr_test, pastoral_districts_path, dir_list_item, year):
    """ Export property specific migration data to the relevant property sub-directory.

    :param prop_curr_test: property and currency date filtered geo-dataframe.
    :param pastoral_districts_path: string object containing the path to the Pastoral Districts directory
    :param dir_list_item: string object containing the shapefile feature type (i.e. lines, points).
    """

    for prop_ in prop_curr_test.PROPERTY.unique():
        # print(prop_)
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

        datetime_object = datetime.strptime(currency, "%Y-%m-%d %H:%M:%S").date()
        date_str = datetime_object.strftime("%Y%m%d")

        output_path = os.path.join(pastoral_districts_path, district, property_name, 'Infrastructure', 'Server_Upload',
                                   str(year), dir_list_item)

        prop_filter = prop_filter.replace(['Not Recorded'], "")
        prop_filter['UPLOAD'] = 'Migration'
        output = os.path.join(output_path, "{0}_Migration_{1}.shp".format(dir_list_item.title(), date_str))
        prop_filter.to_file(output, driver="ESRI Shapefile")

        if 'DELETE' in prop_filter.columns:
            del prop_filter['DELETE']

        prop_filter.to_csv(os.path.join(output_path, "{0}_Migration_{1}.csv".format(dir_list_item.title(), date_str)))
        print(' - Export MIGRATION shapefile to property directory: ', output)


def main_routine(correct_list, pastoral_districts_path, feature_type, year):
    """ Clean and export a copy of the data loaded into the corporate library.
    """

    # call the clean gdf function to remove features.
    clean_gdf = clean_gdf_fn(correct_list)

    # call the property export shapefile function to finalise and export migration reference shapefile to the property
    # sub-directory.
    property_export_shapefile_fn(clean_gdf, pastoral_districts_path, feature_type, year)


if __name__ == '__main__':
    main_routine()
