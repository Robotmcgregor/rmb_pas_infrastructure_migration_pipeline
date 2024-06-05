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
from datetime import datetime
import warnings
from glob import glob
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

    return gdf


def export_data_fn(gdf, faulty_path, feature_type, faulty_output_dir):
    """ Export geo-dataframe and a shapefile to the faulty sub-directory.

    :param gdf: geo-dataframe object
    :param faulty_path: string object containing the path to the xxxxx migration directory
    :param feature_type: string object containing the current shapefile feature type (i.e. points, lines)
    """
    #path_ = "{0}\\Faulty_Pastoral_Infra_{1}.shp".format(faulty_path, feature_type.title())
    print(' - exported to FAULTY:', faulty_output_dir)
    gdf.to_file(faulty_output_dir, driver="ESRI Shapefile")


def main_routine(faulty_list, feature_type, output_dir, faulty_output_dir):
    """ Collate and export the faulty dataframes to the xxx directory.
    """

    faulty_gdf = clean_gdf_fn(faulty_list)

    faulty_path = os.path.join(output_dir, "Faulty", feature_type)

    export_data_fn(faulty_gdf, faulty_path, feature_type, faulty_output_dir)


if __name__ == '__main__':
    main_routine()
