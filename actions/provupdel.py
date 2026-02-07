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
    Disable/remove Metadata Provenance & Version
    This Action is used to harvest Provenance information about current Digital Object and store they into mongodb, 
    inside a collection called do_prov; Version is also written, inside collection do_vers.

# Action-Config setting requirements:
    into config-yaml-workers-file it is possible set the relevant infos about the history of data,
    other conf var are used to connect to db and so on

"""

import os
import datetime
import project.modules.mongomanager

#
# class for action Provenance & Version
#
class provupdel():

    def __init__(self, config, log, session):

        # print("rm provenance ")
        self.log = log
        self.config = config['ACTIONS_CONFIG']['PROVUPDEL']
        self.session = session
        # mongo
        try:
            self.mongo = project.modules.mongomanager.MongoDAO(self.config, self.log)
            self.mongo.connect()

        except Exception as ex:
            self.log.error("Could not connect to Mongo exit..")
            self.log.error(ex)
            self.session['SESSION']['EXIT'] = 1
            return
        
    #
    # Main Provenance MetaData processing
    #
    def do_provupdel(self, file):
        #
        print("do_RM Provenance")
        self.log.info("Starting disable WF PROVENANCE for file %s", file)

        # make a timestamp for metadata time_start
        self.time_start = datetime.datetime.now()

        if self.session['SESSION']['PID_HANDLE']:
            self.handle = self.session['SESSION']['PID_HANDLE']
        else:
            self.log.error('Check SESSION PID_HANDLE  error: values not in session')
            self.session['SESSION']['EXIT'] = 1
            return

        # retrieve previous doc        
        previous_doc = self.mongo.getProvDigitalObject(self.handle)        
        #
        # disable prov document
        enabled = 0
        self.mongo.updateEnableProvByPid(self.handle, enabled)
        self.log.info("OK PROVENANCE document disabled for file %s", file)

        #
        # disable vers document
        if previous_doc:
            print('Disable prov-vers')
            # previous versions returns a list of object ordered by version
            list_previous_versions = self.mongo.getVersionDigitalObject(self.handle)

            # disable all versions
            for i in list_previous_versions:
                # print(i)
                my_id = i['_id']
                self.mongo.updateEnableVersById(my_id, enabled)

            self.log.info("OK VERSION documents disabled for file %s", file)
            