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
    Dublin Core Metadata UPdate DELete
    This Action is used to update or delete a Dublin core metadata document stored into mongodb (i.e. wfc_dc) inside a collection called wf_do;


# Config-Action setting requirements:
    CONFIG:
      MONGO:
        DB_HOST: mongo_db:27017
        DB_NAME: wfc_dc
        USER: user
        PASS: pass
        AUTHENTICATE: false

"""

import datetime

#
# class for action Dublin Core Update-Delete
#
class dublincoreupdel():

    def __init__(self, config, log, session):

        print("dublinCore Update Delete ")
        self.log = log
        self.config = config['ACTIONS_CONFIG']['DUBLINCOREUPDEL']
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
        
    #
    # Main DublinCore MetaData processing
    #
    def do_dublincoreupdel(self, file):
        #
        self.log.info("Starting %s DC-META for file %s" % (self.config['MODE'], file))

        # retrieve doc
        try:
            my_doc = self.mongo.getDublinCoreByFilename(file)
            if my_doc is None:
                self.log.info("NOT json doc for file  %s" % file)
                self.session['SESSION']['EXIT'] = 1
                return
        except Exception as ex:
            self.log.error("Could not compute DublinCoreUPDEL metadata")
            self.log.error(ex)
            self.session['SESSION']['EXIT'] = 1
            return
            
        # retrieve doc _id
        _id = my_doc['_id']
        print(_id)
        #for item in my_doc:
        #    _id = item.get('_id')

        # DELETE
        # do not use this
        if self.config['MODE'] == "DELETE":
            try:
                self.mongo.removeDublinCoreById(_id)
                self.log.info("DC-doc DELETED with _id: " + _id)
            except Exception as e:
                self.log.error('DC-doc DELETE unexpected ERROR: ')
                self.log.error(e)
                self.session['SESSION']['EXIT'] = 1
                return

        # UPDATE - this is used to put in maintenance mode 
        elif self.config['MODE'] == "UPDATE":
            enabled = '0'
            try:
                self.mongo.updateEnableDublinCoreById(_id, enabled)
                # if 'PID_HANDLE' in self.session['SESSION']:
                #    self.mongo.updateHandleDublinCoreById(_id, self.session['SESSION']['PID_HANDLE'])
                self.log.info("DC-doc UPDATE ")
            except Exception as e:
                self.log.error('DC-doc UPDATE unexpected ERROR: ')
                self.log.error(e)
                self.session['SESSION']['EXIT'] = 1
                return

        self.log.info("Completed %s DC-META for file  %s" % (self.config['MODE'], file))
        return
