#!/usr/bin/python3

"""
# Disclaimer:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    any later version.
    This script is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY.

# Copyright: 
    2023 Massimo Fares, INGV - Italy <massimo.fares@ingv.it>; EIDA Italia Team, INGV - Italy  <adaisacd.ont@ingv.it>

# License: 
    GPLv3

# Platform: 
    Linux

# Action-Author: 
    Massimo Fares, INGV - Italy <massimo.fares@ingv.it>

# Action-Description:
    Tag-a-File
    This action tag a given file with some sufffix in order to avoid their use in the meanwhile some actions are performed on it
    Check if the current file  and archived file (into Trust Archive) are equals, if not exit; otherwise tag it and continue policy.


# Config-Action setting requirements:
    TAG_FILE: the value of tag i.e. ".quarantine"
    ARCHIVE_TRUST: "/var/lib/archive/trust/"

"""

import warnings
import filecmp
import os
from project.utils.filechecks import filechecks_util

# needed to silence output warnings for files partially broken
# decommented the following line
# warnings.filterwarnings('ignore', '.*')

#
# class that put a tag on file
#
class tagafile():

    def __init__(self, config, log, session):

        self.log = log
        self.config = config['ACTIONS_CONFIG']['TAGAFILE']
        self.log.info("tagafile start ")
        self.session = session
        self.utils = filechecks_util()

    #
    # Notice! file = path + filename
    #
    def do_tagafile(self, file):

        #
        # STEP 1 Check current file exist
        #
        try:
            if not os.path.isfile(file):
                self.log.error("File no longer exists in checkout directory  %s" % os.path.basename(file))
                self.session['SESSION']['EXIT'] = 1
                return
        except Exception as e:
            self.log.error('Check current file exist error')
            self.log.error(e)
            self.session['SESSION']['EXIT'] = 1
            return

        # Step 1 result ok
        self.log.info("Checkout File is present: OK ")

        #
        # STEP 2 tag a file on archive (if given and archived files are equals)
        #
        archived_file = self.utils.sdsPath(self.config['ARCHIVE_TRUST'], os.path.basename(file))
        print(archived_file)

        try:
            if os.path.isfile(archived_file):
                if filecmp.cmp(archived_file, file):
                    os.rename(archived_file, archived_file + self.config['TAG_FILE'])
                    self.log.info("File in Archive is equals tagged as maintenance file: %s" % os.path.basename(file))
                else:
                    self.log.error("File in Archive is different, somethings goes wrong aborting on %s" % os.path.basename(file))
                    self.session['SESSION']['EXIT'] = 1
                    return
        except Exception as e:
            self.log.error("Checkout vs Archive file error")
            self.log.error(e)
            self.session['SESSION']['EXIT'] = 1
            return

        # Step 2 result ok
        self.log.info("File Tagged with: " + self.config['TAG_FILE'] + " : OK ")
