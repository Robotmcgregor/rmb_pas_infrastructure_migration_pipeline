#!/usr/bin/env python
"""
### Description ###
This script automates the back of the pastoral infrastructure database and map drives.

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
# from __future__ import print_function, division

# import the required modules
import shutil
from datetime import datetime
from glob import glob
import os
from shutil import ignore_patterns


def mk_dir(path_):
    """
    Check if a directory exists; if not, create it.
    :param path_: string object containing the path of the directory.
    """
    if not os.path.isdir(path_):
        os.mkdir(path_)

def main_routine(source_dir, bu_var):
    """
    This script creates a backup copy of the Lib_Corporate, Archive, Other_Data and Property_Edits directories and stores
    them in the U drive Pastoral Infrastructure Backup directory.
    """
    print('Pastoral infrastructure backup is in progress........')


    # create a directory
    dest = os.path.join(source_dir, "Past_Infra_Backup")
    mk_dir(dest)

    # dest = r"U:\Pastoral_Infrastructure\Pastoral_Infrastructure_Backup"

    date = datetime.now()
    str_date = date.strftime("%Y%m%d_%H%M%S")

    primary_dir = os.path.join(dest, "{0}_{1}".format(bu_var, str_date))
    mk_dir(primary_dir)
    print("backup location: ", primary_dir)

    source_lib_corp = os.path.join(source_dir, "Lib_Corporate")
    source_archive = os.path.join(source_dir, "Archive")
    source_other_data = os.path.join(source_dir, "Other_Data")
    source_property_edits = os.path.join(source_dir, "Property_Edits")

    # Create sub dirs
    dest_corp = os.path.join(primary_dir, "Lib_Corporate")
    dest_archive = os.path.join(primary_dir, "Archive")
    dest_other_data = os.path.join(primary_dir, "Other_Data")
    dest_property_edits = os.path.join(primary_dir, "Property_Edits")
    
    # copy directories
    shutil.copytree(source_lib_corp, dest_corp, ignore=ignore_patterns('*.lock'))
    shutil.copytree(source_archive, dest_archive, ignore=ignore_patterns('*.lock'))
    shutil.copytree(source_other_data, dest_other_data, ignore=ignore_patterns('*.lock'))
    shutil.copytree(source_property_edits, dest_property_edits, ignore=ignore_patterns('*.lock'))

    print('Data has been backed up:')
    date = datetime.now()
    str_date = date.strftime("%Y%m%d, %H:%M:%S")
    print(str_date)

    with open(source_dir + '\\backup_log.txt', "w") as file:
        file.write("Data has been backed up: ")
        file.write(str_date)
        file.write('\n')

    file.close()

if __name__ == '__main__':
    main_routine()