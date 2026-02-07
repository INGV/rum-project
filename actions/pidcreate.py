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
    PID-create
    This Action is used to mint a new PID; a credentials file is needed in order to connect to the service, so you must have an account
    (to obtaining an account see: https://servicedesk.surf.nl/wiki/display/WIKI/EPIC+PID+-+Obtaining+an+account)
    it using an evolved version of EUDAT B2HANDLE library: PyHandle; be aware to install it via 'pip install pyhandle'

# Action-Config setting requirements:
    CRED_FILE: relative path for credentials file "i.e. secrets/credentials.json"
    PREFIX: Institutional EPIC prefix "i.e. 22133"
    BASE_LOCATION: base url for local pid landing-page "i.e. https://repo.data.ingv.it"

# How it works:
    firstly check if exist a document into WF-HANDLE (mongo db), if it is enabled go in error;
    then if handle-field is not 'FAKE-PID' do a mint new PID   

"""

from pyhandle.handleclient import PyHandleClient
from pyhandle.clientcredentials import PIDClientCredentials
from pyhandle.handleexceptions import *
import uuid
import os


class pidcreate():

    def __init__(self,  config, log, session):
        # set this action config as a self.config
        self.config = config['ACTIONS_CONFIG']['PIDCREATE']
        self.log = log
        self.session = session
        # mongo
        try:
            import project.modules.mongomanager
            self.mongo = project.modules.mongomanager.MongoDAO(self.config, self.log)
            self.mongo.connect()

        except Exception as ex:
            self.log.error("Could not connect to Mongo exit..")
            self.log.error(ex)
            self.session['SESSION']['EXIT'] = 1
            return

    def do_pidcreate(self, file):
        # PID Minting
        print("do PID-create ")

        enabled = None
        my_handle = None
        my_id = None

        #
        # VERSION CHECK : check if filename is with handle
        #
        self.filevers = os.path.basename(file)
        file_ori = file

        if '#' in self.filevers:
            # it's versioned file
            self.log.info(" PID HANDLE VERSIONED source: " + self.filevers )
            # check session for value inserted by pre-fly
            if 'VERS_FILE' in self.session['SESSION'] and 'PID_FILE' in self.session['SESSION']:
                file = self.session['SESSION']['VERS_FILE']
                filename = self.session['SESSION']['PID_FILE']
                handle = self.session['SESSION']['PID_HANDLE']
            else:
                self.log.error('Check VERS & PID error: values not in session')
                self.session['SESSION']['EXIT'] = 1
                return

            # check if file is already in dc-db
            # 
            previous_doc = self.mongo.getPidDataObject(handle)
        else:
            previous_doc = self.mongo.getFileDataObject(file)


        # doc already into db: OK
        if previous_doc:
            enabled = previous_doc['enabled']
            my_handle = previous_doc['dc_identifier']
            my_id = previous_doc['_id']
            self.session['SESSION']['PID_HANDLE'] = my_handle
            
        # doc not present: error
        else:
            self.log.error("File not in WF-HANDLE can't mint a PID; for: %s" % file)
            self.session['SESSION']['EXIT'] = 1
            return

        # init handle Client
        prefix = self.config['PREFIX']
        # check for dry_run -> w/o registration on global resolver
        if self.config['DRY_RUN'] == False :
            #"""
            credentials_file = self.config['CRED_FILE']
            cred = PIDClientCredentials.load_from_JSON(credentials_file)
            client = PyHandleClient('rest').instantiate_with_credentials(cred)
            #"""
        else:
            self.log.info("WF-HANDLE PID running in DRY_RUN Mode for: %s" % file)

        # FIRST-CHECKIN 
        # enabled=1 and handle=fake-pid  means that is a new ingestion: go haead with mint a new PID
        if enabled == 1 and my_handle == '11099/FAKE-PID':
            self.log.info("File need WF-HANDLE PID, start instantiate_with_credentials process; for: %s" % file)
            
            suffix = str(uuid.uuid1())
            handle = prefix + "/" + suffix
            # location is url of INGV-local landing page
            location = self.config['BASE_LOCATION'] + "/" + handle

            """
            # for future use - to be decide
            # overwrite = False
            # extratypes = {'index': 20, 'type': 'fileId', "data": {"format": "string", "value": os.path.basename(file)}}
            list_of_entries = [{'index': 1, 'type': 'URL', 'data': {"format": "string", "value": location}},
                               {"index": 20, "type": "fileId",
                                "data": {"format": "string", "value": os.path.basename(file)}}]
            
            register_handle_json( handle, list_of_entries, overwrite)
            """

            # register Handle
            try:
                if self.config['DRY_RUN'] == False :
                    # live 
                    pid = client.register_handle(handle, location)
                else:    
                    # dry
                    pid = handle

                self.log.info("PID CREATED handle: %s  with URL: %s" % (pid, location))
                self.session['SESSION']['PID_HANDLE'] = pid
                self.session['SESSION']['PID_FILE'] = file
            except Exception as e:
                self.log.error('PID CREATE unexpected ERROR: ')
                self.log.error(e)
                self.session['SESSION']['EXIT'] = 1
                return
            
            # put the right handle into db

            my_ret = self.mongo.updateHandleDublinCoreById( my_id, handle)

            return

        # SECOND+ CHECKIN    
        # if enabled with a valid PID it's a re-check-in: restore a valid url resolver
        if enabled == 1 and my_handle != '11099/FAKE-PID':
            self.log.info("File enabled in WF-HANDLE with a valid PID: restore a valid url ")
            # restore the correct URL
            location = self.config['BASE_LOCATION'] + "/" + my_handle

            # modify Handle
            try:
                if self.config['DRY_RUN'] == False :
                    # live
                    updatepid = client.modify_handle_value(my_handle, URL=location)
                else:    
                    # dry
                    updatepid = my_handle
                
                self.log.info("PID UPDATED handle: %s with URL: %s" % (updatepid, location))
            except Exception as e:
                self.log.error('PID UPDATED unexpected ERROR: ')
                self.log.error(e)
                # self.session['SESSION']['EXIT'] = 1
                return

