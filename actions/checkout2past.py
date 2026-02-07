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
    Checkout-to-Past
    This Action is used to move the given file into Past Archive; if there are some errors exit.

# Config-Action setting requirements:
    ARCHIVE_WORKING: "/var/lib/archive/working/"
    TARGET_SDS: allow to specify if the target structure is an SDS archive i.e. "false"

"""

import os
import shutil
from project.utils.filechecks import filechecks_util

#
# class for action that move a data-file from checkout to working area
#
class checkout2past():

    def __init__(self, config, log, session):

        print("checkout2past ")
        self.log = log
        self.session = session
        self.config = config['ACTIONS_CONFIG']['CHECKOUT2PAST']
        self.utils = filechecks_util()

    #
    # Final move file
    #
    def _move_file(self, file):

        #
        #  move from Checkout area to past Archive 

        
        message = "CHECK-OUT-OFFLINE complete, will move file into Past Archive " + self.config['ARCHIVE_TARGET'] +" - "+  file
        self.log.info(message)

        only_file = os.path.basename(file)

        #  make the rigth path if sds structure is configured via 'target-sds'
        if self.config['TARGET_SDS'] == True:
            target_path = self.utils.sdsPath(self.config['ARCHIVE_TARGET'], only_file)
        else:
            target_path = os.path.join(self.config['ARCHIVE_TARGET'], only_file)
        print(target_path)

        # prep source trust archive path to be deleted
        source_path = self.utils.sdsPath(self.config['ARCHIVE_TRUST'], only_file)
        print(source_path)

        # move current file to past
        try:
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            shutil.move(file, target_path)
            self.log.info("Move to Past Area - OK" + only_file)
            # self.session['SESSION']['EXIT'] = 1
        except Exception as e:
            self.log.error('Move to Past Area ERROR on : ' + only_file)
            self.log.error(e)
            self.session['SESSION']['EXIT'] = 1
            return

        # delete on trust archive
        try:
            os.remove(source_path)
            self.log.info("Remove from Trust Archive - OK" + only_file)
            # self.session['SESSION']['EXIT'] = 1
        except Exception as e:
            self.log.error('Remove from Trust Archive ERROR  : ' + only_file)
            self.log.error(e)
            self.session['SESSION']['EXIT'] = 1
            return


    #
    # Main Move file from checkout to past area
    #
    def do_checkout2past(self, file):

        print('checkout2past')
        self._move_file(file)
        return
