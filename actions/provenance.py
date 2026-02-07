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
    Collect Metadata Provenance & Version
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
class provenance():

    def __init__(self, config, log, session):

        # print("provenance ")
        self.log = log
        self.config = config['ACTIONS_CONFIG']['PROVENANCE']
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
    def do_provenance(self, file):
        #
        print("do_Provenance")
        self.log.info("Starting processing WF PROVENANCE for file %s", file)

        # make a timestamp for metadata time_start
        self.time_start = datetime.datetime.now()

        if 'PID_HANDLE' in self.session['SESSION']:
            self.handle = self.session['SESSION']['PID_HANDLE']
        else:
            self.log.error('PID_HANDLE error: values not in session')
            self.session['SESSION']['EXIT'] = 1
            return
                
        #
        # VERSION CHECK : check if filename is with handle
        #
        self.filevers = os.path.basename(file)
        file_ori = file
        previous_doc = None
        # load previous prov doc
        #
        previous_doc = self.mongo.getProvDigitalObject(self.handle)

        if '#' in self.filevers:
            # it's versioned file
            self.log.info(" PROVENANCE VERSIONED source: " + self.filevers )
            
            # check session for value inserted by previous action
            if 'VERS_FILE' in self.session['SESSION'] and 'PID_FILE' in self.session['SESSION']:
                # set values from session
                file = self.session['SESSION']['VERS_FILE']
                filename = self.session['SESSION']['PID_FILE']
            else:
                self.log.error('Check VERS & PID error: values not in session')
                self.session['SESSION']['EXIT'] = 1
                return
       
            # load previous prov doc
            # 
            #previous_doc = self.mongo.getProvDigitalObject(self.handle)
            version = True
        else:
            # no prev doc: is a first checkin
            #previous_doc = None
            version = False
        
        #
        # SECOND+ CHECKIN
        if version:
            print('SECOND+ CHEKIN')
            # previous versions returns a list of object ordered by version
            list_previous_versions = self.mongo.getVersionDigitalObject(self.handle)

            # take last version
            for i in list_previous_versions:
                # print(i)
                previous_version = i
                previous_id = i['_id']
                previous_filename = i['schema_file']['name'] 

            self.last_version = previous_version['dc_hasVersion']
            self.last_filename = previous_filename
            self.session['SESSION']['LAST_FILENAME'] = previous_filename
            self.current_version = str(int(previous_version['dc_hasVersion']) + 1)
            
            # create current version
            vers_doc = self._createDigitalObjectVersion(file)

            # store current version
            try:   
                if vers_doc is not None:
                    self.mongo.storeVersionDigitalObject(vers_doc)
                else:
                    self.log.info("NOT Version json doc for file  %s" % file)
                    self.session['SESSION']['EXIT'] = 1
                    return

                # update last version filename into meta-db/version-archive; append version number like : NN.SSSS.CCC.LLL.D.YYYY.JJJ-V
                file_version = previous_filename + '-' + self.last_version

                # update location for resolver 'schema_file_position':  resolver-address + handle +"#version="+current_version
                location_version = self.config['RESOLVER'] + self.handle + '#version=' + self.last_version
                self.mongo.updateVersionDigitalObject(previous_id, file_version, location_version)

                # enable prov doc
                enabled = 1
                self.mongo.updateEnableProvByPid(self.handle, enabled)
                self.log.info("OK PROVENANCE document disabled for file %s", file)

                # enable version docs
                # retrieve previous versions returns a list of object ordered by version
                list_previous_versions = self.mongo.getVersionDigitalObject(self.handle)
                # enable all versions
                for i in list_previous_versions:
                    # print(i)
                    my_id = i['_id']
                    self.mongo.updateEnableVersById(my_id, enabled)

                # put last version into session
                self.session['SESSION']['VERSION'] = self.last_version

            except Exception as e:
                self.log.error('WF-PROV-VERSION doc ADD/UPDATE unexpected ERROR: ')
                self.log.error(e)
                self.session['SESSION']['EXIT'] = 1
                return

            self.log.info("WF-PROV-VERSION doc: ADD-new and UPDATE-last ")
            return


        # FIRST TIME CHECKIN or RELOAD
        elif previous_doc is None:
            print('FIRST CHECKIN - OR RELOAD')
            self.current_version = '0'
            try:
                # create provenance document - only first time
                prov_doc = self._createDigitalObjectProv(file)

                # store provenance
                if prov_doc is not None:
                    self.mongo.storeProvDigitalObject(prov_doc)
                else:
                    self.log.error("NOT Provenance json doc for file  %s" % file)
                    self.session['SESSION']['EXIT'] = 1
                    return

                # create current version
                vers_doc = self._createDigitalObjectVersion(file)

                # store current version    
                if vers_doc is not None:
                    self.mongo.storeVersionDigitalObject(vers_doc)
                else:
                    self.log.error("NOT Version json doc for file  %s" % file)
                    self.session['SESSION']['EXIT'] = 1
                    return
                
            except Exception as ex:
                self.log.error("Could not compute First Provenance metadata")
                self.log.error(ex)
                self.session['SESSION']['EXIT'] = 1
                return

            self.log.info("Completed processing WF-PROV for file  %s" % file)
            return
        else:
            self.log.info("Already prov record present on WF-PROV for file  %s" % file)
            return

    #
    # Assemble Provenance Meta and build a Json Doc for Mongo
    #
    def _createDigitalObjectProv(self, file):
        
        # Create document with provenance metadata
        self.log.info("prep Provenance document")

        usage = {
                'schema_SoftwareApplication': self.config['USAGE']
                } 

        # provenance document
        document = {
            'enabled': 1,
            'dc_identifier': self.handle,
            'dcterms_isPartOf': self.config['DC_PARTOF'],
            'prov_generatedAtTime' :  self.time_start,
            'prov_wasAttributedTo': self.config['ATTRIBUTE_TO'],
            'prov_usage': usage
            }

        return document

    #
    # Retrieve Version Meta and build a Json Doc for Mongo
    #
    def _createDigitalObjectVersion(self, file):

        if self.session['SESSION']['COVERAGE_X'] and self.session['SESSION']['COVERAGE_Y']:
            dc_terms_spatial = {
                'x' : self.session['SESSION']['COVERAGE_X'],
                'y' : self.session['SESSION']['COVERAGE_Y'],
                'z' : self.session['SESSION']['COVERAGE_Z'] 
            }
        else:
            self.log.error("NOT COVERAGE info in session for file  %s" % file)
            self.session['SESSION']['EXIT'] = 1
            return

        schema_file = {
            'name': os.path.basename(file),
            'position': self.config['RESOLVER'] + self.handle
        }

        # Retrieve station informations
        sta = str(os.path.basename(file).split('.')[1])
        net = str(os.path.basename(file).split('.')[0])

        # generate url to stationxml as phisical source of data :: level=response&net=IV&station=ACER
        primary_source = self.config['STATION_ENDPOINT'] + "level=response" + "&net=" + net + "&sta=" + sta 

        # use net description to describe data provider
        if "NET_DESCRIPTION" in self.session['SESSION']:
            schema_Organization = self.session['SESSION']['NET_DESCRIPTION']
        else:
            schema_Organization = self.config['GENERATED_BY']['ORGANIZZATION']

        # generated with this sw from this data provider with this periodicty
        wasGeneratedBy = {
                'prov_hadPrimarySource': primary_source,
                'schema_SoftwareApplication': self.config['GENERATED_BY']['SW_APP'], 
                'schema_Organization': schema_Organization,
                'dcterms_accrualPeriodicity': self.config['GENERATED_BY']['PERIODICITY']
                }

                
        # version document (prov:: wasRevisionOf)
        document = {
            'enabled': 1,
            'dc_identifier': self.handle,
            'dc_hasVersion': self.current_version,
            'schema_startDate': self.time_start,
            'schema_Organization': self.config['REVISION_OF']['ORGANIZZATION'],
            'prov_SoftwareAgent': self.config['REVISION_OF']['SW_APP'],
            'dc_terms_spatial' : dc_terms_spatial,
            'schema_file': schema_file,
            'prov_wasGeneratedBy': wasGeneratedBy

            }

        return document
