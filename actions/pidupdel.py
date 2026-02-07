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
    PID-UPdate-DELete
    This Action is used to modify or delete an existent PID;
    it using an evolved version of EUDAT B2HANDLE library, the PyHandle; be aware to install via 'pip install pyhandle'
    If it is not possible minting the PID exit.

# Config-Action setting requirements:
    CRED_FILE: relative path for credentials "i.e. secrets/credentials.json" file
    PREFIX: Institutional EPIC prefix "i.e. 22133"
    BASE_LOCATION: base url for pid resolver "i.e. https://www.ingv.it"
    MODE: is this action to UPDATE or DELETE ?

"""

from pyhandle.handleclient import PyHandleClient
from pyhandle.clientcredentials import PIDClientCredentials
from pyhandle.handleexceptions import *
import uuid


#
# class for action Pid Handle Update-Delete
#
class pidupdel():

    def __init__(self,  config, log, session):
        # set this action config as a self.config
        self.config = config['ACTIONS_CONFIG']['PIDUPDEL']
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

    def do_pidupdel(self, file):
        # PID Manage
        print("do PID-UPdateDELete ")

        cred = PIDClientCredentials.load_from_JSON(self.config['CRED_FILE'])
        client = PyHandleClient('rest').instantiate_with_credentials(cred)
        pid = self.mongo.getPIDfromFile(file)
        self.session['SESSION']['PID_HANDLE'] = pid
        if self.config['DRY_RUN'] == True:
            self.log.info("PID NOT UPDATED - DRY-RUN Mode - skip updel" )
            return

        
        """
        # DELETE -
        # BE AWARE: likely this function never be used; PID-HANDLE have to be persistent and immutable for decades
        if self.config['MODE'] == "DELETE":
            try:
                client.delete_handle(pid)
                self.log.info("PID DELETED with handle: " + pid)
                self.session['SESSION']['PID_HANDLE'] = pid
                self.session['SESSION']['PID_FILE'] = file
            except Exception as e:
                self.log.error('PID DELETE unexpected ERROR: ')
                self.log.error(e)
                self.session['SESSION']['EXIT'] = 1
                return
        """

        # UPDATE - this is used to put resolver in maintenance mode on CHECKOUT operaton
        if self.config['MODE'] == "UPDATE":
            
            # set the URL for maintenance
            location = self.config['MAINT_LOCATION']
            try:
                updatepid = client.modify_handle_value(pid, URL=location)
                self.log.info("PID UPDATED handle: %s with URL: %s" % (updatepid, location))
                self.session['SESSION']['PID_HANDLE'] = pid
                self.session['SESSION']['PID_FILE'] = file
            except Exception as e:
                self.log.error('PID UPDATED unexpected ERROR: ')
                self.log.error(e)
                # self.session['SESSION']['EXIT'] = 1
                return

        return
