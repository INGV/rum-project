#! /usr/bin/env python
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

# Module-Author:
    Massimo Fares, INGV - Italy <massimo.fares@ingv.it>


#
#  Mod. by AdA-IS group (adaisacd.ont@ingv.it) 2021
#
  
"""
import os
from pymongo import MongoClient

#
# Data Access Object  for MongoDB
#
#
class MongoDAO():

    def __init__(self, config, log):

        # print("mongo start")

        self.log = log
        self.config = config
        self.host = self.config['MONGO']['DB_HOST']
        self._connected = False
        self.client = None
        self.db = None

    #
    # connect to MongoDB
    #
    def connect(self):
        
        if self._connected:
            return

        self.client = MongoClient(self.host)
        self.db = self.client[self.config['MONGO']['DB_NAME']]

        if self.config['MONGO']['AUTHENTICATE']:
            self.db.authenticate(self.config['MONGO']['USER'], self.config['MONGO']['PASS'])

        self._connected = True

    #
    # Disconnect to MongoDB
    #
    def disconnect(self):
        
        if self._connected:
            self.client.close()
            self._connected = False
            return True
        else:
            return False

        


    # -------- Provenance -----------
    #
    # DB name: wf_prov
    # collections: do_prov; do_vers



    #
    # get do_prov document by pid
    #
    def getProvDigitalObject(self, handle):

        return self.db.do_prov.find_one({'dc_identifier': handle}) 

    #
    # get do_vers document by pid oredered by version
    #
    def getVersionDigitalObject(self, handle):

        # return self.db.do_vers.find({'dc_identifier': handle}).sort({'version':1})
        return self.db.do_vers.find({'dc_identifier': handle}).sort([('version',1)]) 

    #
    # update Version file-name by _id
    #
    def updateVersionDigitalObject(self, my_id, file_version, location_version):

        self.db.do_vers.update_one({'_id': my_id}, {"$set": {"schema_file.name": file_version}}, upsert=False)
        self.db.do_vers.update_one({'_id': my_id}, {"$set": {"schema_file.position": location_version}}, upsert=False)

    #
    # update enable in do_prov collection
    #
    def updateEnableProvByPid(self, handle, enabled):

        self.db.do_prov.update_one({'dc_identifier': handle}, {"$set": {"enabled": enabled}}, upsert=False)

    #
    # update enable in do_vers collection
    #
    def updateEnableVersById(self, my_id, enabled):

        self.db.do_vers.update_one({'_id': my_id}, {"$set": {"enabled": enabled}}, upsert=False)


    #
    # _store DigitalObjectProv
    # store data provenance into do_prov collection
    #
    def storeProvDigitalObject(self, obj):
        # print("store Provenance data object")
        try:
            self.db.do_prov.insert_one(obj)
        except Exception as ex:
            self.log.error("error on insert Provenance")
            self.log.error(ex)
            print("ERROR on mongo insert")

        return 

    #
    # _store DigitalObjectVersion
    # store data version into do_vers collection
    #
    def storeVersionDigitalObject(self, obj):
        # print("store Version data object")
        try:
            self.db.do_vers.insert_one(obj)
        except Exception as ex:
            self.log.error("error on insert Version")
            self.log.error(ex)
            print("ERROR on mongo insert")

        return 

    # -------- DublinCore -----------
    #
    # DB name: wf_hand
    # collection: wf_do


    #
    # get wf_do FileDataObject
    #
    def getFileDataObject(self, file):

        return self.db.wf_do.find_one({'fileId': os.path.basename(file)})

    #
    # get wf_do PidDataObject
    #
    def getPidDataObject(self, pid):

        return self.db.wf_do.find_one({'dc_identifier': pid})    

    #
    # get PID from FileDataObject
    #
    def getPIDfromFile(self, file):

        doc = self.db.wf_do.find_one({'fileId': os.path.basename(file)})        
        return doc['dc_identifier']

    #
    # store data object to wf_do collection
    #
    def _storeFileDataObject(self, obj):

        return self.db.wf_do.insert_one(obj)    

    #
    # _store FileDataObject
    # store data object to wf_do collection
    #
    def storeWFDataObject(self, obj):
        # print("store data object")
        try:
            self.db.wf_do.insert_one(obj)
        except Exception as ex:
            self.log.error("error on insert")
            self.log.error(ex)
            print("ERROR on mongo insert")

        return 

    #
    # get a DublinCore By Filename
    #
    def getDublinCoreByFilename(self, file):

        return self.db.wf_do.find_one({'fileId': os.path.basename(file)})

    #
    # update Date Enabled in DublinCore collection
    #
    def updateEnableDublinCoreById(self, id, enabled):

        self.db.wf_do.update_one({'_id': id}, {"$set": {"enabled": enabled}}, upsert=False)

    #
    # update fileId in DublinCore collection
    #
    def updateFilenameDublinCoreById(self, id, filename):

        self.db.wf_do.update_one({'_id': id}, {"$set": {"fileId": filename}}, upsert=False)    

    #
    # update Date Availability in DublinCore collection
    #
    def updateDateDublinCoreById(self, id, date_time_obj):

        self.db.wf_do.update_one({'_id': id}, {"$set": {"dcterms_available": date_time_obj}}, upsert=False)
    #
    # update PID-HANDLE in DublinCore collection
    #
    def updateHandleDublinCoreById(self, id, handle):

        self.db.wf_do.update_one({'_id': id}, {"$set": {"dc_identifier": handle}}, upsert=False)

    #
    # removes documents from DublinCore collection
    #
    def removeDublinCoreById(self, id):

        self.db.wf_do.delete_one({'_id': id})

    #
    # DB name: wf_hand
    # collection: net_info

    def getNetInfoByNet(self, net):

        return self.db.net_info.find_one({'net': net})



    # -------- WFCatalog -----------
    #
    # DB name: wfrepo
    # collections: daily_streams ; c_segments


    # 
    # stores daily and hourly granules to collections
    #
    def _storeGranule(self, stream, granule):

        if granule == 'daily':
            
            return self.db.daily_streams.insert_one(stream).inserted_id
        elif granule == 'hourly':
          return self.db.hourly_streams.insert_one(stream).inserted_id
    
    # 
    # removes documents all related to ObjectId
    #
    def removeDocumentsById(self, id):
       
        self.db.daily_streams.delete_one({'_id': id})
        self.db.hourly_streams.delete_many({'streamId': id})
        self.db.c_segments.delete_many({'streamId': id})
    
    # 
    # Saves a continuous segment to collection
    #
    def storeContinuousSegment(self, segment):
        # print(segment)
        try:
            self.db.c_segments.insert_one(segment)
        except Exception as ex:
            print(ex)

    # 
    # returns all documents that include this file in the metadata calculation
    #
    def getDailyFilesById(self, file):
        
        return self.db.daily_streams.find({'files.name': os.path.basename(file)}, {'files': 1, 'fileId': 1, '_id': 1})

    #
    # get a Document By Filename
    #
    def getDocumentByFilename(self, file):

        return self.db.daily_streams.find({'fileId': os.path.basename(file)})

    #
    # get One Document By Filename
    #
    def getDocumentByFilenameOne(self, file):

        return self.db.daily_streams.find_one({'fileId': os.path.basename(file)})
