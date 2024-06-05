# !/usr/bin/env python

"""
### Description ###

The RMB Pastoral Infrastructure Pipeline is the second half of the Pastoral Infrastructure Workflow.
This Python Pipeline should only be run following the successful completion of the RMB Pastoral Infrastructure
Transition Pipeline, and only once the RMB Manager has verified the data. This pipeline creates a backup prior to
and following the data update, appends the new data into the corporate data set, archives the existing data in the
corporate data set to the archive data set, overwrites and overwrites the snapshot dataset based on the current date.

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
from datetime import date
import argparse
import shutil
import warnings
from glob import glob
import sys
import pandas as pd
from shutil import ignore_patterns

warnings.filterwarnings("ignore")


def cmd_args_fn():
    p = argparse.ArgumentParser(
        description='''Transfer finalised RMB Infrastructure shapefiles and maps from the transition dir to the 
        Lib_Corporate directory. Separate old spatial data and previous maps to the Archive dir.''')

    p.add_argument('-x', '--output_dir', type=str, help='Directory path for outputs.',
                   default=r"P:\Pipelines\Output\rmb_infrastructure_migrate")

    p.add_argument("-pd", "--pastoral_districts",
                   help="Enter path to the Pastoral_Districts directory in the Spatial/Working drive)",
                   default=r'P:\Pastoral_Districts')

    p.add_argument('-y', '--year', type=int, help='Enter the year (i.e. 2001).')

    p.add_argument("-r", "--remote_desktop", help="Working on the remote_desktop? - Enter remote_auto, remote, "
                                                  "local or offline.", default="remote")
    p.add_argument('-t', '--transition_dir', type=str, help='Directory path for outputs.',
                   default=r"P:\Pastoral_Infrastructure\Transition")

    p.add_argument('-a', '--assets_dir', type=str, help='Directory path containing required shapefile structure.',
                   default=r'E:\DEPWS\code\prod\rangeland_monitoring\rmb_migration_pipeline\assets')

    p.add_argument('-i', '--pastoral_infrastructure', type=str,
                   help='Enter the path to the HIDDEN Pastoral Infrastructure directory - SECURE LOCATION.',
                   #default=r"P:\Data\past_infra_orig"
                   default=r"U:\rmb\past_infra_orig")

    p.add_argument('-w', '--working_pastoral_infrastructure', type=str,
                   help='Enter the path to the WORKING DRIVE Pastoral Infrastructure directory - U DRIVE.',
                   default=r"P:\Pastoral_Infrastructure")


    cmd_args = p.parse_args()

    if cmd_args.year is None:
        p.print_help()

        sys.exit()

    return cmd_args


def user_id_fn():
    """ Extract the users id stripping of the adm.
.
    :return final_user: string object containing the NTG user id. """

    # extract user name
    home_dir = os.path.expanduser("~")
    _, user = home_dir.rsplit('\\', 1)

    final_user = user[3:]

    return final_user


def export_file_path_fn(primary_output_dir, final_user):
    """ Create an export directory 'user_YYYMMDD_HHMM' at the location specified in command argument primary_output_dir.

        :param primary_output_dir: string object containing the path to the export directory (command argument).
        :param final_user: string object containing the NTG user id
        :return output_dir_path: string object containing the newly created directory path for all retained exports. """

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    output_dir_path = primary_output_dir + '\\' + str(final_user) + '_' + str(date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(
        date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message.
    try:
        shutil.rmtree(output_dir_path)

    except:
        pass

    # create folder titled 'tempFolder'
    os.makedirs(output_dir_path)

    return output_dir_path


def migration_dir_folders_fn(direc):
    """ Create directory tree within the transitory directory based on shapefile type.

    :param direc: string object containing the path to the transitory directory.
    """

    if not os.path.exists(direc):
        os.mkdir(direc)


def assets_search_fn(search_criteria, folder):
    """ Searches through a specified directory "folder" for a specified search item "search_criteria".

    :param search_criteria: string object containing a search variable including glob wildcards.
    :param folder: string object containing the path to a directory.
    :return files: string object containing the path to any located files or "" if none were located.
    """
    path_parent = os.path.dirname(os.getcwd())
    assets_dir = (path_parent + '\\' + folder)

    files = ""
    file_path = (assets_dir + '\\' + search_criteria)
    # print('file_path: ', file_path)

    for files in glob(file_path):
        # print(file_path, 'located.')
        pass

    return files


def dir_folders_2_fn(direc, directory_list, type_):
    """
    Create directory tree within the transitory directory based on shapefile type.

    :param type_: string object containing the sub-directory name.
    :param direc: string object containing the path to the transitory directory.
    :param directory_list: list object containing the shapefile types.
    """

    if not os.path.exists(direc):
        os.mkdir(direc)

    previous_dir = "{0}\\{1}".format(direc, type_)
    check_dir = os.path.isdir(previous_dir)
    if not check_dir:
        os.mkdir(previous_dir)

    path_list = []
    for i in directory_list:
        shapefile_dir = ('{0}\\{1}'.format(previous_dir, i))
        path_list.append(shapefile_dir)
        if not os.path.exists(shapefile_dir):
            os.mkdir(shapefile_dir)
            print(" - created: ", shapefile_dir)


    return previous_dir


def dir_folders_3_fn(direc, directory_list, type_):
    """
    Create directory tree within the transitory directory based on shapefile type.

    :param type_: string object containing the sub-directory name.
    :param direc: string object containing the path to the transitory directory.
    :param directory_list: list object containing the shapefile types.
    """

    if not os.path.exists(direc):
        os.mkdir(direc)

    previous_dir = "{0}\\{1}".format(direc, type_)
    check_dir = os.path.isdir(previous_dir)
    if not check_dir:
        os.mkdir(previous_dir)

    path_list = []
    for i in directory_list:
        shapefile_dir = ('{0}\\{1}'.format(previous_dir, i))
        path_list.append(shapefile_dir)
        if not os.path.exists(shapefile_dir):
            os.mkdir(shapefile_dir)
            print(" - created: ", shapefile_dir)


def next_subfolder_fn(path_to_parent):
    try:
        return next(os.walk(path_to_parent))[1]
    except StopIteration:
        return []


def main_routine():
    """ This pipeline transfers finalised infrastructure data from the transition directory to the migration directory.
    Data has been checked by a relevantly skilled person in the transition directory. This script will confirm

    """

    # print('step1_1_initiate_mapping_pipeline.py INITIATED............')

    # read in the command arguments
    cmd_args = cmd_args_fn()
    pastoral_districts_path = cmd_args.pastoral_districts
    transition_dir = cmd_args.transition_dir
    output_dir = cmd_args.output_dir
    pastoral_infrastructure = cmd_args.pastoral_infrastructure
    working_pastoral_infrastructure = cmd_args.working_pastoral_infrastructure
    year = cmd_args.year


    print('='*50)
    corporate_infrastructure = os.path.join(pastoral_infrastructure, "Lib_Corporate", "Data", "ESRI")
    archive_infrastructure = os.path.join(pastoral_infrastructure, "Archive", "Data", "ESRI")
    for_migration = os.path.join(transition_dir, "For_Migration")

    print('LOCATION OF FOR MIGRATION DIRECTORY: ', for_migration)
    print('LOCATION OF CORPORATE LIBRARY DIRECTORY: ', corporate_infrastructure)
    print('=' * 50)


    import step2_7_backup_infrastructure
    step2_7_backup_infrastructure.main_routine(pastoral_infrastructure, "Pre")

    #pastoral_estate = assets_search_fn("NT_Pastoral_Estate.shp", "assets\\shapefile")
    asset_search = os.path.join('assets','shapefile')
    pastoral_estate = assets_search_fn("NT_Pastoral_Estate.shp", asset_search)


    # directory_list = ["Points", "Lines", "Polygons", "Paddocks"]
    directory_list = ["points", "lines", "polygons", "paddocks"]

    final_user = user_id_fn()

    # call the export_file_path_fn function to create an export directory.
    primary_output_dir = export_file_path_fn(output_dir, final_user)

    # create subdirectories within the export directory
    final_migration_output_dir = dir_folders_2_fn(primary_output_dir, directory_list, 'final_migration')

    faulty_output_dir = dir_folders_2_fn(primary_output_dir, directory_list, 'faulty')
    print("faulty_output_dir: ", faulty_output_dir)


    import step2_2_search_folders
    step2_2_search_folders.main_routine(
        pastoral_districts_path, transition_dir, corporate_infrastructure, archive_infrastructure, year, output_dir,
    faulty_output_dir)

    
    # delete files within for migration directory
    for feature_type in directory_list:
        for_migration_path = os.path.join(transition_dir, "for_migration", feature_type)
        #print("for_migration_path: ", for_migration_path)


        if glob("{0}\\*".format(for_migration_path)):
            print('-' * 30)
            print("Deleting...")
            for file in glob("{0}\\*".format(for_migration_path)):

                # Handle errors while calling os.remove()
                try:
                    os.remove(file)
                    print(" - ", file)
                except:
                    print("Error while deleting file ", file)
            print('-' * 50)

        else:
            print('-'*30)
            print("No files in the ", feature_type, " FOR MIGRATION directory")
            print('-' * 50)

    import step2_6_pdf_maps
    step2_6_pdf_maps.main_routine(
        year, transition_dir, pastoral_infrastructure)

    # import step2_7_backup_infrastructure
    # step2_7_backup_infrastructure.main_routine(pastoral_infrastructure, "Post")

    import step2_8_create_annual_snapshot
    step2_8_create_annual_snapshot.main_routine(pastoral_infrastructure)

    import step2_9_copy_to_working
    step2_9_copy_to_working.main_routine(pastoral_infrastructure, working_pastoral_infrastructure)


if __name__ == '__main__':
    main_routine()
