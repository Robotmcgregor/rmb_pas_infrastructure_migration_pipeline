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
import shutil
import warnings
from glob import glob
from datetime import datetime
warnings.filterwarnings("ignore")


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

    return prop_list


def pdf_maps_upload_path_fn(prop_list, year):
    """ Create a path to to the Server Upload and Download sub-directories for each property and return them as a list.

    :param prop_list: list object containing the path to all property sub-directories.
    :return upload_list: list object containing the path to each properties Server_Upload sub-directory.
    :return download_list: list object containing the path to each properties Server_Upload sub-directory.
    """
    upload_list = []
    for prop_path in prop_list:
        upload_path = os.path.join(prop_path, 'Infrastructure', 'Server_Upload', str(year), 'pdf_maps')
        upload_list.append(upload_path)

    return upload_list


def main_routine(year, transition_dir, corporate_working):
    """ Collect finalised pdf maps from the for migration sub-directory and transfer them to the corporate
    sub-directory, archiving any property specific maps located in the corporate drive.

    """

    # create a path to the for_migration, corporate and archive sub-directories.
    for_migration = os.path.join(transition_dir, 'For_Migration', 'Pdf_Maps')
    corporate = os.path.join(corporate_working, "Lib_Corporate", "Products", "Maps")
    archive = os.path.join(corporate_working, "Archive", "Maps", "A0_MonSites")

    if not os.path.exists(for_migration):
        os.mkdir(for_migration)

    date = datetime.now()
    string_date = date.strftime("%Y%m%d_%H%M%S")

    if glob("{0}\\*.pdf".format(for_migration)):
        for pdf_map in glob("{0}\\*.pdf".format(for_migration)):
            print(' - Located: ', pdf_map)
            print('=' * 50)
            file_list = pdf_map.split('\\')
            orig_file = file_list[-1]
            # print('original file: ', orig_file)
            file_label = orig_file[:-9]
            # print(file_label)
            property_file_list = []
            if glob("{0}\\{1}*.pdf".format(corporate, file_label)):
                for corp_map in glob("{0}\\{1}*.pdf".format(corporate, file_label)):
                    print('corp_map: ', corp_map)
                    corp_file_list = corp_map.split('\\')
                    corp_file = corp_file_list[-1]
                    print("orig_file: ", orig_file)
                    print("corp_file: ", corp_file)
                    if orig_file == corp_file:
                        print('SAME the same file already exists in the corporate library.')
                        print(' - pdf_map: ', pdf_map)
                        print('copied to: ', corp_map)
                        shutil.copy(pdf_map, corp_map)

                        path_list, file_ = pdf_map.rsplit('\\', 1)
                        print('path_list: ', path_list)
                        print('file_: ', file_)


                    else:
                        print('DIFFERENT - the file is new to to corporate library')
                        archive_output = os.path.join(archive, corp_file)
                        print('-' * 50)
                        print(' - corp_map: ', corp_map)
                        print('MOVED to: ', archive_output)
                        shutil.move(corp_map, archive_output)

                        corp_file_list = corp_map.split('\\')
                        corp_file = corp_file_list[-1]


                        print('-' * 50)
                        corp_output = os.path.join(corporate, orig_file)
                        print(' - pdf_map: ', pdf_map)
                        print('copied to: ', corp_output)
                        shutil.copy(pdf_map, corp_output)

                        # path_list, file_ = pdf_map.rsplit('\\', 1)


            else:
                path_list, file_ = pdf_map.rsplit('\\', 1)
                print('path_list: ', path_list)
                print('file_: ', file_)

                print('Not in Corporate Library')
                corp_output = os.path.join(corporate, orig_file)
                shutil.copy(pdf_map, corp_output)
                print("Copied from: ", pdf_map)
                print("To: ", corp_output)

                # # text_doc = "{0}\\map_transfer.txt".format(path_list)
                # if os.path.exists(text_doc):
                #     print('exists')
                #     # with open(text_doc, 'a') as f:
                #     #     # f = open(text_doc, "a")
                #     #     f.write(orig_file)
                #     #     f.write(
                #     #         ' is new and was transferred into the corporate library on ')
                #     #     f.write(string_date)
                #     #     f.write('.\n')
                #     #     f.close()
                # else:
                #     f = open(text_doc, "w+")
                #     f.write(orig_file)
                #     f.write(' is new and was transferred into the corporate library on ')
                #     f.write(string_date)
                #     f.write('.\n')
                #     f.close()


        print('Deleting: ')

        # Handle errors while calling os.remove()
        try:
            os.remove(pdf_map)
            print(" - ", pdf_map)
        except:
            print("Error while deleting file ", pdf_map)

    else:
        print('No maps for transfer...')


if __name__ == '__main__':
    main_routine()
