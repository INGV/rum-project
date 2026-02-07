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
    Dublin Core Metadata extractor
    This Action is used to compose a Dublin core metadata document to store into mongodb (i.e. wfc_dc) inside a collection called wf_do;
    it using mongomanager module and it's used to generate a new metadata record into db
    it also use EIDA Station webservices in order to retrieve station infos

# Action-Config setting requirements:
    via config it is possible set the relevant infos about the owner of data;
    other conf var are used to connect to db and station webservices
    CONFIG:
      DC_TITLE: 'INGV_Repository',
      DC_SUBJECT: 'mSEED, waveform, seismic data',
      DC_CREATOR: 'INGV EIDA',
      DC_CONTRIBUTOR: 'network operator',
      DC_PUBLISHER: 'EIDA ITALIA',
      DC_TYPE: 'seismic waveform',
      DC_FORMAT: 'MSEED',
      DC_RIGHTS: 'open access',
      DC_PARTOF: 'EIDA wf-catalog metadata'
      MONGO:
        DB_HOST: mongo_db:27017
        DB_NAME: wfc_dc
        USER: user
        PASS: pass
        AUTHENTICATE: false
      STATION_ENDPOINT: http://webservices.ingv.it/fdsnws/station/1/query?
      HTTP_CONNECTION: webservices.ingv.it
      LOG_FILE: dublincore.log

"""

import os
import http.client
import datetime

#
# class for actions Dublin Core
#
class dublincore():

    def __init__(self, config, log, session):

        # print("dublinCore ")
        self.log = log
        self.config = config['ACTIONS_CONFIG']['DUBLINCORE']
        self.session = session
        # mongo
        try:
            import project.modules.mongomanager
            self.mongo = project.modules.mongomanager.MongoDAO(self.config, self.log)
            self.mongo.connect()

        except Exception as ex:
            self.log.error("Could not connect to Mongo exit..")
            self.log.error(ex)
            self.session['SESSION']['SESS_ID']['EXIT'] = 1
            return
        
    #
    # Main DublinCore MetaData processing
    #
    def do_dublincore(self, file):
        #
        print("do_DublinCore")
        self.log.info("Starting processing DC-META for file %s", file)
        
        enabled = None
        my_id = None

        #
        # VERSION CHECK : check if filename is with handle
        #
        self.filevers = os.path.basename(file)
        file_ori = file

        if '#' in self.filevers:
            # it's versioned file
            self.log.info(" DC HANDLE VERSIONED source: " + self.filevers )
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

        if not os.path.isfile(file):
            self.log.info("File no longer exists in archive %s" % filename)
            return

        # SECOND+ CHECKIN
        if previous_doc:
            enabled = previous_doc['enabled']
            my_id = previous_doc['_id']
            self.session['SESSION']['COVERAGE_X'] = previous_doc['dc_coverage_x']
            self.session['SESSION']['COVERAGE_Y'] = previous_doc['dc_coverage_y']
            self.session['SESSION']['COVERAGE_Z'] = previous_doc['dc_coverage_z']
            
            if enabled == 1:
                self.log.info("File already enabled in DC-META skip: %s" % file)
                # self.session['SESSION']['FILE_EXIST'] = 1
                return
            else:
                self.log.info("File enabling in DC-META: %s" % file)
                enabled = 1
                try:
                    # 
                    self.mongo.updateFilenameDublinCoreById(my_id, filename)
                    self.mongo.updateEnableDublinCoreById(my_id, enabled)
                    self.log.info("DC-doc UPDATE ")
                except Exception as e:
                    self.log.error('DC-doc UPDATE unexpected ERROR: ')
                    self.log.error(e)
                    self.session['SESSION']['EXIT'] = 1
                    return


        # FIRST-CHECKIN
        else:
            try:
                my_doc = self._createDataObject(file)
                if my_doc is not None:
                    self.mongo.storeWFDataObject(my_doc)
                elif self.session['SESSION']['EXIT'] == 1:
                    self.session['SESSION']['EXIT'] = 0
                else:
                    self.log.info("NOT json doc for file  %s" % file)
            except Exception as ex:
                self.log.error("Could not compute DublinCore metadata in first checkin")
                self.log.error(ex)
                self.session['SESSION']['EXIT'] = 1
                return

            self.log.info("Completed processing DC-META for file  %s" % file)
            return

    #
    # Retrieve Dublin Core Meta and build a Json Doc for Mongo
    #
    def _createDataObject(self, file):

        # check if file already exist into DublinCore collection
        currentCursor = self._getFileDataObjectID(file)
        # if update is set, do update (in progress)
        if not self.config['UPDATE_IF_EXIST']:
            if currentCursor and currentCursor != '11099/FAKE-PID':
                self.log.error(file+" :: DC Meta of this file already present!")
                self.log.info(" Nothing to do continue next action")
                self.session['SESSION']['EXIT'] = 1
                return
        else:
            # @TODO: manage update for time being return
            self.log.error(file + " :: DC Meta of this file already present!")
            self.log.error("Update DC Meta for this file")
            return

        # Retrieve station informations
        sta = str(os.path.basename(file).split('.')[1])
        net = str(os.path.basename(file).split('.')[0])
        stationinfo = self._getDataStation(net, sta)

        # put coverage in session
        self.session['SESSION']['COVERAGE_X'] = float(stationinfo['lat'])
        self.session['SESSION']['COVERAGE_Y'] = float(stationinfo['lon'])
        self.session['SESSION']['COVERAGE_Z'] = float(stationinfo['ele'])

        # set start/end time of file if session exist otherwise left station's values
        if 'STARTIME' in self.session['SESSION']:
            stationinfo['start'] = self.session['SESSION']['STARTIME']
            stationinfo['end'] = self.session['SESSION']['ENDTIME']
            self.log.info("Assigned start/end time from file, start at: %s " % stationinfo['start'])
        else:
            # @TODO: retrieve info if not set in session
            # @TODO: something like this: start_time = wfcollector._getDateFromFile(file)
            # @TODO: but it's expensive (we have to read again all file)
            self.log.info("WARNING: Assigned start/end time from station (no session value), start at: %s " % stationinfo['start'])

        # set HANDLE if PID_HANDLE exist
        if 'PID_HANDLE' in self.session['SESSION']:
            pid_handle = self.session['SESSION']['PID_HANDLE']
            self.log.info("Assigned PID-Handle for this file : %s " % pid_handle)
        else:
            pid_handle = '11099/FAKE-PID'
            self.log.info("WARNING: NO PID-Handle for file  %s" % os.path.basename(file))

        # Create document with dublin-core metadata
        self.log.info("prep document")
        document = {
          'enabled': 1,
          'fileId': os.path.basename(file),
          'dc_identifier': pid_handle,
          'dc_title': self.config['DC_TITLE'],
          'dc_subject': self.config['DC_SUBJECT'],
          'dc_creator': self.config['DC_CREATOR'],
          'dc_contributor': self.config['DC_CONTRIBUTOR'],
          'dc_publisher': self.config['DC_PUBLISHER'],
          'dc_type': self.config['DC_TYPE'],
          'dc_format': self.config['DC_FORMAT'],
          'dc_date': datetime.datetime.now(),
          'dc_coverage_x': float(stationinfo['lat']),
          'dc_coverage_y': float(stationinfo['lon']),
          'dc_coverage_z': float(stationinfo['ele']),
          'dc_coverage_t_min': stationinfo['start'],
          'dc_coverage_t_max': stationinfo['end'],
          'dc_rights': self.config['DC_RIGHTS'],
          'dcterms_available': datetime.datetime.now(),
          'dcterms_dateAccepted': datetime.datetime.now(),
          'dcterms_isPartOf': self.config['DC_PARTOF']
        }
        return document

    #
    # retrieve data stations via webservices
    #
    def _getDataStation(self, net, sta):

        # Retrieve informations from EIDA station webservice be aware! the response must be in text format (not xml)
        # example:
        # conn = http.client.HTTPConnection("webservices.rm.ingv.it", 80)
        # query = "http://webservices.rm.ingv.it/fdsnws/station/1/query?net=IV&sta=AMUR&format=text"
        # result = "#Network | Station | Latitude | Longitude | Elevation | SiteName | StartTime          | EndTime \n
        #              IV    | AMUR    | 40.9071  | 16.6041   |  443      |Altamura  |2005-08-03T15:46:00|          "
        conn = http.client.HTTPConnection(self.config['HTTP_CONNECTION'], 80)
        query = self.config['STATION_ENDPOINT'] + "net=" + net + "&sta=" + sta + "&format=text"
        try:
            conn.connect()
            conn.request("GET", query)
            response = conn.getresponse()
            e = response.read()
            conn.close()
        except Exception as ex:
            self.log.error("ERROR with Station webservices, set EXIT")
            self.log.error(ex)
            self.session['SESSION']['EXIT'] = 1
            return
        mystation = {"lat": e.decode('utf-8').split('|')[9], "lon": e.decode('utf-8').split('|')[10], "ele": e.decode('utf-8').split('|')[11], "start": e.decode('utf-8').split('|')[13], "end": e.decode('utf-8').split('|')[14]}

        return mystation

    #
    # _getFileDataObject id
    #
    def _getFileDataObjectID(self, file):
        # print("_getFileDataObjectID : document")
        # If the metadata of file exists in the collection return id
        try:
            doc = self.mongo.getFileDataObject(file)
            if doc:
                return doc['dc_identifier']
        except Exception as ex:
            self.log.error("ERROR with Mongo, set EXIT")
            self.log.error(ex)
            #self.session['SESSION']['EXIT'] = 1
            print("ERROR with Mongo, set EXIT")
            return

        return
