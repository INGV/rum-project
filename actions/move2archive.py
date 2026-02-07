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
    Move-to-Archive
    This Action is used to move the given file into Trusted Arcive; remove a tagged file on archive (i.e. '.quarantine').
    If there are some errors exit.

# Config-Action setting requirements:
    TAG_FILE: the value of tag i.e. ".quarantine"
    ARCHIVE_TRUST: "/var/lib/archive/trust/"

"""

import os
import shutil
from project.utils.filechecks import filechecks_util

#
# class for action that move a data-file from checkin to archive
#
class move2archive():

    def __init__(self, config, log, session):

        self.log = log
        self.session = session
        self.config = config['ACTIONS_CONFIG']['MOVE2ARCHIVE']
        self.utils = filechecks_util()

    #
    # Final move file
    #
    def _move_file(self, file, target_archive, sds=True):

        
        # sds switch
        if sds:
            target_path = self.utils.sdsPath(target_archive, os.path.basename(file))
        else:
            target_path = target_archive + os.path.basename(file) + self.config['TAG_WARNING']

        self.log.info("move file start target_path : " + target_path)
        #
        #  move from Checkin area to target Archive;
        try:
            # save current version
            #print("move/copy file in target_path ")
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            if self.config['MV_NOT_CP'] == True:
                shutil.move(file, target_path)
                self.log.info("MOVE to Target Archive - OK " + target_path)  # + os.path.basename(file)
            else:
                shutil.copy(file, target_path)
                self.log.info("COPY to Target Archive - OK " + target_path)  # + os.path.basename(file)

            self.log.info("remove tagged file")    
            # remove TAGGED (i.e. .maintenance) file on trusted archive (if exist) and put in version archive
            if os.path.isfile(target_path+self.config['TAG_FILE']):  
                # print("exist maint file - not a versioned: " + target_path+self.config['TAG_FILE'])              
                self.log.info("There is a tagged file " + target_path+self.config['TAG_FILE'])
                #copy to versioned archive
                version_path = self.utils.sdsPath(self.config['ARCHIVE_VERSION'], os.path.basename(file) )
                os.makedirs(os.path.dirname(version_path), exist_ok=True)
                shutil.copy(target_path+self.config['TAG_FILE'], version_path+'-'+self.session['SESSION']['VERSION'])
                # delete original file
                os.remove(target_path+self.config['TAG_FILE'])
                self.log.info("tagged file removed, the new one is in place")

                # if session version exist, save original(.maintenance) file into version archive
            elif 'VERSION' in self.session['SESSION']:
                version_path = self.utils.sdsPath(self.config['ARCHIVE_VERSION'], self.session['SESSION']['LAST_FILENAME'])
                maint_path = self.utils.sdsPath(target_archive,self.session['SESSION']['LAST_FILENAME']+self.config['TAG_FILE']) 
                              
                # print("exist maint file but versioned - move maint-path into version-path: version_path: " + version_path + " maint_path: " + maint_path)
                os.makedirs(os.path.dirname(version_path), exist_ok=True)
                if os.path.isfile(maint_path):                
                    self.log.info("There is a last version tagged file " + maint_path)

                    shutil.copy(maint_path, version_path+'-'+self.session['SESSION']['VERSION'])
                    self.log.info("version file saved on archive-version with: " + version_path+'-'+self.session['SESSION']['VERSION'])
                    # delete original file
                    os.remove(maint_path)
                    self.log.info("version tagged file removed, the new one is in place")
                    # self.session['SESSION']['EXIT'] = 1
                else:
                    self.log.error("generic error on version")
            else:
                self.log.info("there isn't tagged maint file")

        except Exception as e:
            self.log.error('Move to Archive Error : ' + os.path.basename(file))
            self.log.error(e)
            # self.session['SESSION']['EXIT'] = 1
            return

    #
    # Main: Move file from source to target archive
    #
    def do_move2archive(self, file):
        #
        print('do_move2archive')

        #
        # VERSION CHECK : check if filename is with handle
        #
        self.filevers = os.path.basename(file)
        file_ori = file

        if '#' in self.filevers:
            # it's versioned file
            self.log.info(" MOVE VERSIONED source: " + self.filevers )
            # check session for value inserted by pre-fly
            if 'VERS_FILE' in self.session['SESSION']and 'PID_FILE' in self.session['SESSION']:
                file = self.session['SESSION']['VERS_FILE']
                filename = self.session['SESSION']['PID_FILE']
            else:
                self.log.error('Check VERS & PID error: values not in session')
                self.session['SESSION']['EXIT'] = 1
                return

        # file not exist yet: OK! move in trust archive
        if 'FILE_EXIST' not in self.session['SESSION']:
            message = "POLICY complete, will move file into Production-Trusted Archive " + self.config['ARCHIVE_TRUST'] + " - " + os.path.basename(file)
            self.log.info(message)
            self._move_file(file, self.config['ARCHIVE_TRUST'])
            # remove if version
            if '#' in self.filevers:
                os.remove(file_ori)
            return
        # file exist somethig went wrong move in warning area
        else:
            message = "CHECK-IN NOT complete, move file into Warning Archive " + self.config['ARCHIVE_WARNING'] + " - " + os.path.basename(file)
            self.log.info(message)
            self._move_file(file, self.config['ARCHIVE_WARNING'])
            if '#' in self.filevers:
                os.remove(file_ori)
            return
