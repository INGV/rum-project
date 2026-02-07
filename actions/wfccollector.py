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
    WFC-Collector
    This Action is used to extract EIDA WF_catalog metadata from mseed file;
    it using a modified version of EIDA WFCatalogCollector.py as a module (actually a couple of module WFCatalogManager and MongoManager)
    If it is not possible extract metadata exit.

# Config-Action setting requirements:
    In this context all the configs are the same of WFCatalogCollector python script from EIDA;
    BTW in this specific action we need some extra conf-var in order to simulate the script params, they are:
      ARGS:
        csegs: true
        flags: true
        file: none

    Entire WFC-Collector Module config structure:

    VERSION: 0.5.0
    VERSION_DATE: '2021-07-07'
    ARCHIVE: EIDA NODE
    PUBLISHER: OBSPY MSEED-QC
    STRUCTURE: SDS
    ARGS:
        csegs: true
        flags: true
        file: none
    MONGO:
        ENABLED: true
        DB_HOST: mongodb:27017
        DB_NAME: wfrepo
        USER: user
        PASS: pass
        AUTHENTICATE: false
        ALLOW_DOUBLE: false
    ARCHIVE_ROOT: "/var/lib/archive/incoming/"
    DEFAULT_LOG_FILE: WFCatalog-collector.log
    PROCESSING_TIMEOUT: 120
    ENABLE_DUBLIN_CORE: false
    FILTERS:
        WHITE:
        - "*"
        BLACK: []

"""
from project.modules.wfcatalogmanager import WFCatalogCollector
import os
from project.modules.mongomanager import MongoDAO
import time



class wfccollector():

    def __init__(self,  config, log, session):
        # set this action config as a self.config
        self.config = config['ACTIONS_CONFIG']['WFCCOLLECTOR']
        self.log = log
        self.session = session
        # mongo
        self.mongo = MongoDAO(self.config, self.log)
        """
        if self.config['MONGO']['ENABLED']:
            import project.modules.mongomanager
            self.mongo = project.modules.mongomanager.MongoDAO(self.config, self.log)
            self.mongo.connect()

        else:
            self.mongo = ''
        """
        # wfcatalog collector legacy args
        self.parsedargs = self.config['ARGS']

    def do_wfccollector(self, file):
        # WFC Collector
        print("do WFC-collector ")

        #
        # VERSION CHECK : check if filename is with handle
        #
        self.filevers = os.path.basename(file)

        if '#' in self.filevers:
            # it's versioned file
            self.log.info(" WFC METADATA for VERSIONED source: " + self.filevers )
            # check session for value inserted by pre-fly
            if 'VERS_FILE' in self.session['SESSION'] and 'PID_FILE' in self.session['SESSION']:
                file = self.session['SESSION']['VERS_FILE']
                filename = self.session['SESSION']['PID_FILE']
            else:
                self.log.error('Check VERS & PID error: values not in session')
                self.session['SESSION']['EXIT'] = 1
                return
                
        if not os.path.isfile(file):
            self.log.info("File no longer exists in archive %s" % filename)
            return

        # wfcatalog collector legacy args
        self.parsedargs['file'] = file


        #  while loop until connected/Meta-collected
        while True:

            try:

                # print("start meta extraction")
                # do metadata extraction
                self.log.info("called collect WF CATALOG METADATA for : " + os.path.basename(file))

                self.mongo.connect()
                # spawn wfc_collector
                wfc_collector = WFCatalogCollector(self.parsedargs, self.config, self.mongo, self.log)
                # wfcatalog collector legacy
                mylist = wfc_collector.getFileList()
                wfc_collector.collectMetadata(file)
                self.log.info(" WF METADATA for source: " + os.path.basename(file) + " is: OK")
                break
            except Exception as ex:
                self.log.error("Could not compute WF metadata: check wfcollector or mongodb")
                self.log.error(ex)
                print("ERROR could not compute WF metadata")
                del wfc_collector
                time.sleep(3)
                #self.session['SESSION']['EXIT'] = 1
                #return

        return
