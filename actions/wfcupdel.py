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
    WFC-Update-Delete
    This Action is used to update/delete EIDA WF_catalog metadata from database according to current mseed file;


# Config-Action setting requirements:
    In this context all the configs are the same of WFCatalogCollector python script from EIDA;
    BTW in this specific action we need some extra conf-var in order to simulate the script params, they are:
      ARGS:
        csegs: true
        flags: true
        file: none

"""
from project.modules.wfcatalogmanager import WFCatalogCollector
import os


class wfcupdel():

    def __init__(self,  config, log, session):
        # set this action config as a self.config
        self.config = config['ACTIONS_CONFIG']['WFCUPDEL']
        self.log = log
        self.session = session
        # mongo
        if self.config['MONGO']['ENABLED']:
            import project.modules.mongomanager
            self.mongo = project.modules.mongomanager.MongoDAO(self.config, self.log)
            self.mongo.connect()
        else:
            print("ERROR! Mongo MUST be enabled")
            self.session['SESSION']['EXIT'] = 1
            return
        # wfcatalog collector legacy args
        self.parsedargs = self.config['ARGS']

    # DO_ACTION
    def do_wfcupdel(self, file):
        if self.config['MODE'] == 'RESTORE':
            self.wfcupdater(file)
        elif self.config['MODE'] == 'REMOVE':
            self.wfcremover(file)

        return

    # UPDATE -> RESTORE doc deleted previously
    def wfcupdater(self, file):
        # WFC Update
        print("wfcupdater")
        # wfcatalog collector legacy args
        self.parsedargs['file'] = file
        # self.parsedargs['update'] = True
        # spawn wfc_collector
        wfc_updater = WFCatalogCollector(self.parsedargs, self.config, self.mongo, self.log)
        # wfcatalog collector legacy
        mylist = wfc_updater.getFileList()
        # do metadata update
        self.log.info("called updater WF CATALOG METADATA for : " + os.path.basename(file))
        try:
            wfc_updater.collectMetadata(file)
            self.log.info(" WF UPDATE METADATA for source: " + file + " is: OK")
            if self.config['IF_OK_EXIT']:
                self.session['SESSION']['EXIT'] = 1
                return
        except Exception as ex:
            self.log.error("Could not update WF metadata")
            self.log.error(ex)
            self.session['SESSION']['EXIT'] = 1
            return
        return

    # DELETE -> delete doc in order to avoid meta pubblished on missing file
    def wfcremover(self, file):
        # WFC Remover
        print("wfcremover")

        # @TODO: copy mongo-doc into historical db for future version-system

        # wfcatalog collector legacy args
        self.parsedargs['file'] = file
        self.parsedargs['delete'] = True
        # spawn wfc_collector
        wfc_remover = WFCatalogCollector(self.parsedargs, self.config, self.mongo, self.log)
        # wfcatalog collector legacy
        print("get file list")
        mylist = wfc_remover.getFileList()
        print(mylist)
        # do metadata remove
        self.log.info("called remover WF CATALOG METADATA for : " + os.path.basename(file))
        try:
            wfc_remover._deleteFiles()
            self.log.info(" WF REMOVE METADATA for source: " + file + " is: OK")
        except Exception as ex:
            self.log.error("Could not remove WF metadata")
            self.log.error(ex)
            # self.session['SESSION']['EXIT'] = 1
            return
        return
