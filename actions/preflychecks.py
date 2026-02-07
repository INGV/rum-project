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
    PreFly-Checks
    This action perform some test on the given file in order to check if exist, not duplicate in Trust-archive and check if it is tagged.
    If file isn't present exit.


# Config-Action setting requirements:
    TAG_FILE: the value of tag i.e. ".quarantine"
    ARCHIVE_TRUST: "/var/lib/archive/trust/"

"""

import warnings
import filecmp
import os
import shutil
from project.utils.filechecks import filechecks_util

# needed to silence output warnings for files partially broken
# decommented the following line
# warnings.filterwarnings('ignore', '.*')

#
# class that execute pre-fly checks on given file
#
class preflychecks():

    def __init__(self, config, log, session):

        self.log = log
        self.config = config['ACTIONS_CONFIG']['PREFLYCHECKS']
        self.log.info("preflychecks start ")
        self.session = session
        self.utils = filechecks_util()
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
    #  PreFly Checks processing
    #
    # Notice! 
    # 1° time: file = path + filename
    #
    # 2° time: file = path + (filename # handle) --> (/OT/OT03/EHN.D/OT.OT03..EHN.D.2024.006#11099.963f39b2-af94-11ee-84c6-0242ac120007)
    #
    def do_preflychecks(self, file):

        filename = os.path.basename(file)
        print("do_preflychecks for: " + filename)

        #
        # TEST 1 Check given file exist
        #
        try:
            if not os.path.isfile(file):
                self.log.error("File no longer exists in check-in directory  %s" % filename)
                self.session['SESSION']['EXIT'] = 1
                return
        except Exception as e:
            self.log.error('Check current file exist error')
            self.log.error(e)
            print(e)
            self.session['SESSION']['EXIT'] = 1
            return

        # Test 1 result ok
        self.log.info("Check File is present: OK ")

        #
        # VERSION CHECK : check if filename is with handle
        #
        filevers = os.path.basename(file)
        file_ori = file

        if '#' in filevers:
            # it's versioned file
            filename = filevers.split("#")[0]
            handle = filevers.split("#")[1]
            handle = handle.replace(".", "/")
            print(filename)
            print(handle)
            # set handle and filename in session
            self.session['SESSION']['PID_HANDLE'] = handle
            self.session['SESSION']['PID_FILE'] = filename

            # make a working copy with sds filename only
            
            self.log.info(" PRE_FLY for VERSIONED source: " + filename + " start copy")
            if self.config['SCRATCH_SDS'] == True:
                file_v = self.utils.sdsPath(self.config['ARCHIVE_SCRATCH'], filename)            
            else:
                file_v = os.path.join( self.config['ARCHIVE_SCRATCH'], filename)

            try:
                os.makedirs(os.path.dirname(file_v), exist_ok=True)
                shutil.copy(file, file_v) 
            except Exception as e:
                self.log.error("Copy working file error")
                self.log.error(e)
                self.session['SESSION']['EXIT'] = 1
                return

            self.log.info(" working copy done: " + file_v )
            self.session['SESSION']['VERS_FILE'] = file_v
            # substitute original file with working-copy
            file = file_v




        #
        # TEST 1.5 check if authoritative network
        #
        

        if self.config['CHK_AUTH'] == True:
            
            # make a mongo query to retrieve net info
            # then put info into session
            #
            net = filename.split(".")[0]
            net_info = self.mongo.getNetInfoByNet(net)
            

            if net_info == None:
                # reject file

                message = "File not Authoritative, moved file into REJECT " + self.config['ARCHIVE_NO_AUTH'] + filename
                self.log.error(message)
                print("ERROR File not Authoritative")
                self.session['SESSION']['EXIT'] = 1

                # Goto or Exit ?
                if self.config['IF_NO_AUTH_GOTO'] != 'none':
                    if self.config['IF_NO_AUTH_GOTO'] == 'exit':
                        self.session['SESSION']['EXIT'] = 1
                        return

                    # jump to the GOTO action and keep the current file
                    self.session['SESSION']['GOTO'] = self.config['IF_NO_AUTH_GOTO']
                    self.session['SESSION']['NO_AUTH'] = True
                    self.session['SESSION']['AUTH_ERROR'] = net
                    self.session['SESSION']['EXIT'] = 0
                    return

                
                rejected = self.utils.reject_file(filename, file, self.config['ARCHIVE_NO_AUTH'], self.config['NO_AUTH_SDS'], self.config['TAG_NO_AUTH'])
                if rejected != 0:
                        self.log.error("Reject ERROR")
                        self.log.error(rejected)
                else:
                    # remove if version
                    if '#' in filevers:
                        os.remove(file_ori)

                return

            else:
                self.session['SESSION']['NET_CODE'] = net_info['net']
                self.session['SESSION']['NET_DESCRIPTION'] = net_info['description']

        # Test 1.5 result ok
        self.log.info("Check File is Authoritative: OK ")

        #
        # TEST 2 Check current file NOT IN archive
        #
        archived_file = self.utils.sdsPath(self.config['ARCHIVE_TRUST'], filename)
        #print("archived_file : " + archived_file)
        if self.config['CHK_ARCHIVE'] == True:

            try:
                if os.path.isfile(archived_file):
                    self.log.error("Check Archive file error: File is present in Archive, rejected! something went wrong aborting on %s" % os.path.basename(file))
                    self.session['SESSION']['EXIT'] = 1
                    print("ERROR: file is present into archive")
                    
                    rejected = self.utils.reject_file(filename, file, self.config['ARCHIVE_WARNING'], self.config['WARNING_SDS'], self.config['TAG_WARNING'])
                    if rejected != 0:
                        self.log.error("Reject ERROR")
                        self.log.error(rejected)
                    else:
                        # remove if version
                        if '#' in filevers:
                            os.remove(file_ori)    

                    return
            except Exception as e:
                self.log.error("Check Archive file present error")
                self.log.error(e)
                self.session['SESSION']['EXIT'] = 1
                return

        # Test 2 result ok
        self.log.info("Check File NOT in Archive: OK ")

        #
        # TEST 3 Check if archived-tagged and current file are equals *** be-aware! for testing put NOT in filecompare()
        #
        if self.config['CHK_TAGGED'] == True:

            try:
                if os.path.isfile(archived_file + self.config['TAG_FILE']):

                    # compare two files; NOTE: remove 'not' in filecompare if needed by policy 
                    if not filecmp.cmp(archived_file + self.config['TAG_FILE'], file):
                        self.log.error(
                            "Check Archive file error: file tagged with: " + self.config['TAG_FILE'] + "and current file are equals, rejected! something went wrong aborting on %s" % os.path.basename(
                                file))
                        self.session['SESSION']['EXIT'] = 1
                        
                        rejected = self.utils.reject_file(filename, file, self.config['ARCHIVE_WARNING'], self.config['WARNING_SDS'], self.config['TAG_WARNING'])
                        if rejected != 0:
                            self.log.error("Reject ERROR")
                            self.log.error(rejected)
                        else:
                            # remove if version
                            if '#' in filevers:
                                os.remove(file_ori)

                        return

            except Exception as e:
                self.log.error("Check Archived-tagged and current file are NOT equals error")
                self.log.error(e)
                self.session['SESSION']['EXIT'] = 1
                return

        # Test 3 result ok
        self.log.info("Check tagged and current file NOT equals: OK ")
        return
