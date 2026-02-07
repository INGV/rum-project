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
    Checkout-to-Working
    This Action is used to move the given file into Working Archive; if there are some errors exit.

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
class checkout2working():

    def __init__(self, config, log, session):

        print("checkout2archive ")
        self.log = log
        self.session = session
        self.config = config['ACTIONS_CONFIG']['CHECKOUT2WORKING']
        self.utils = filechecks_util()

    #
    # Final move file
    #
    def _move_file(self, file):

        #
        #  move from Checkout area to Working Archive 

        # append Handle to filename to obtain this format: filename#prefix.handle
        current_pid = self.session['SESSION']['PID_HANDLE']
        current_pid = current_pid.replace('/' , '.')
        file_pid = os.path.basename(file) + "#"+ current_pid
        print(file_pid)
        message = "CHECK-OUT complete, will move file into Working Archive " + self.config['ARCHIVE_WORKING'] +" - "+  file_pid
        self.log.info(message)

        #  make the rigth path if sds structure is configured via 'target-sds'
        if self.config['TARGET_SDS'] == True:
            target_path = self.utils.sdsPath(self.config['ARCHIVE_WORKING'], file_pid)
        else:
            target_path = os.path.join(self.config['ARCHIVE_WORKING'], file_pid)
        print(target_path)
        
        try:
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            shutil.move(file, target_path)
            self.log.info("Move to Working Area - OK" + file_pid)
            # self.session['SESSION']['EXIT'] = 1
        except Exception as e:
            self.log.error('Move to Working Area  : ' + file_pid)
            self.log.error(e)
            self.session['SESSION']['EXIT'] = 1
            return

    #
    # Main Move file from checkout to working area
    #
    def do_checkout2working(self, file):

        print('checkout2working')
        self._move_file(file)
        return
