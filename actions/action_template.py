#########################################
# This is a template to build an action.#
#########################################

"""

==========
# SHA-BANG
==========
#!/usr/bin/python3

============
# LEGAL-INFO
============
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

============
# TEACH-INFO
============
# Action-Description:

    A description af this Action 
    i.e.: is used to move the given file into Working Archive; if there are some errors exit.

# Config-Action setting requirements:
    
    the params that are essential for this action; 

    i.e. :
    DB-NAME: myDB
    to specify a name of working DB

# file config required:

   a file named config-<name-of-action>.yaml-workers must be present into /config directory contenent the required params

"""

#================
# IMPORT SECTION:
#================
#
# import the only modules that are necessary to this action
# each action could have a utility or specific modules
# respectively in /utils and /modules
#

# i.e:
import os
import shutil
from project.utils.filechecks import filechecks_util



"""
#==================
# CLASS for ACTION:
#==================
# this is a main-class for this action; 
# important! the class name  must have the same name (case sensitive) of python file name (i.e. action_template.py -> class action_template():)
# 
#
"""

class action_template():

    #
    # Constructor:
    # each action must have a constructor that must accept 3 framework's data-structure: log; config; session.
    #
    #

    def __init__(self, config, log, session):

        print("action_template called")
        self.log = log
        self.session = session
        # set action own config :: self.config = config['ACTIONS_CONFIG']['<ACTION_NAME>']
        # that is the same saved in config-action_template.yaml-workers overwritten by rule -if needed.
        self.config = config['ACTIONS_CONFIG']['ACTION_TEMPLATE']
        self.utils = filechecks_util()

    #=============
    # MAIN METHOD:
    #=============
    # each action must have a main method called "do_<action-name>" (same of class-name) 
    # and must accept 1 param 'file' that is  path/namefile '
    #
    #

    def do_action_template(self, file):

        print('action_template start')
        """
        call your functions
        
        i.e.:
        """
        my_first_step = self._first(file)
        
        # end of action
        # eventually set some session param if needed
        #
        # i.e.:
        if not my_first_step:
            self.session['SESSION']['EXIT'] = 1
            self.log.info("Hoo noo! your setting are False")
            return False
            
        else:
            self.session['SESSION']['EXIT'] = 0
            self.log.info("Enjoy! your setting are True")
            return True


    #===========
    # FUNCTIONS:
    #===========
    #
    def _first(self, file):

        message = "do_action_template for file: " +  os.path.basename(file)
        self.log.info(message)
        if not self.config['DO_ACTION']:
            return False
        else:
            return True


#
# OFF-RUM usage/testing Action class
#
if __name__ == "__main__":
    import logging

    # -----------------------------
    # Setup basic logging
    # -----------------------------
    log = logging.getLogger("action_test")
    log.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    log.addHandler(handler)

    # -----------------------------
    # Test configuration
    # -----------------------------
    config = {
        "SOME_VARS": {
            "SOME_URL": "http://my_host.int.ingv.it:14000",
            "SOME_PATH": "/user/jhon.doe"
        },
        "OTHER_VARS": {
            "OTHER_PATH": "/usr/src/code/project/spare/massimo.fares.keytab",
            "OTHER_USER": "jhon.doe@MY_REALM.INT.INGV.IT"
        }
    }

    # Fake session object
    session = {"SESSION": {"USER": "test"}}

    # Path of file
    file = "/usr/src/code/project/spare/test_file.txt"

    # Execute  action
    actioner = action_template(log=log, config=config, session=session)

    result = actioner.do_action_template(file)

    if result:
        log.info("Action completed successfully.")
    else:
        log.error("Action failed.")
