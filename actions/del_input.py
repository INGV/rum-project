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
    Del Input
    This Action is used to delete input file.
    If there are some errors exit.

# Config-Action setting requirements:

    
"""

import os
import shutil
from project.utils.filechecks import filechecks_util

#
# class for action that move a data-file from checkin to archive
#
class del_input():

    def __init__(self, config, log, session):

        self.log = log
        self.session = session
        self.config = config
        self.utils = filechecks_util()

    #
    # Main: DELETE file from source 
    #
    def do_del_input(self, file):
        #
        print('do_del_input')
        result = self.utils.delete_file(file)

        if result == 0:
            self.log.info("Input file correctly deleted: "+ os.path.basename(file))
        else:
            self.log.error("Error! input file not deleted: "+ os.path.basename(file))
            self.log.error(result)

        return
