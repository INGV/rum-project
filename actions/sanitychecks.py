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
    Sanity-Checks
    This action perform a series of test on the given file in order to spot if the file is a valid Mseed Data-File.
    If it is bad some actions could be execute via specify the relevant key: IF_BAD_GOTO; otherwise move into Bad directory 
    and exit (default behavior)


# Config-Action setting requirements:
    STATION_ENDPOINT: url for station service
    ARCHIVE_BAD: "/var/lib/archive/bad/" target of bad files
    IF_BAD_GOTO: jump to action if it is a bad file, otherwise exit.
    TYPE_CODE: mseed type file (i.e. Data, Log, etc..)
    BAND_CODE: mseed sensor frequency 

"""

from obspy import read
import warnings
import datetime
import shutil
import os
from project.utils.filechecks import filechecks_util

# needed to silence output warnings for files partially broken
warnings.filterwarnings('ignore', '.*')


#
# class that execute sanity checks on file
#
class sanitychecks():

    def __init__(self, config, log, session):

        self.log = log
        self.config = config['ACTIONS_CONFIG']['SANITYCHECKS']
        self.session = session
        self.log.info("sanitychecks initialized ")
        self.error_code = ""
        self.utils = filechecks_util()

    #
    #  Sanity Checks processing
    #
    # Notice! file = path + filename
    #
    def do_sanitychecks(self, file):

        filename = os.path.basename(file)
        good = True 
        
        #
        # VERSION CHECK : check if filename is with handle
        #
        self.filevers = os.path.basename(file)

        if '#' in self.filevers:
            # it's versioned file
            self.log.info(" SANITY for VERSIONED source: " + self.filevers )
            # check session for value inserted by pre-fly
            if 'VERS_FILE' in self.session['SESSION'] and 'PID_FILE' in self.session['SESSION']:
                file = self.session['SESSION']['VERS_FILE']
                filename = self.session['SESSION']['PID_FILE']
            else:
                self.log.error('Check VERS_FILE  error: not in session')
                good = False
                self._move_file(filename, file, good)
                return
        
        if not os.path.isfile(file):
            self.log.info("File no longer exists in archive %s" % filename)
            return

        mseed = file
        st = None       
        band_code =  self.config['BAND_CODE']
        band_file_code = filename.split('.')[3][0]

        self.log.info("SANITY-Checks START for :" + filename)
        print("do_sanitychecks for: " + filename)

        #
        # TEST 0 - 1 Check ZERO/Broken file
        #
        # check if the file is empty or not valid
        try:
            st = read(mseed)
        except Exception as e:
            self.log.error('Check ZERO file error: ')
            self.log.error(e)
            print('ERROR Check ZERO file: the file is empty or unreadable')
            self.error_code = "0"
            good = False
            self._move_file(filename, file, good)
            return

        if st is None:
            self.log.error('Check Broken file: FAILED ')
            print('ERROR Check Broken file: the file is unusable')
            self.error_code = "1"
            good = False
            self._move_file(filename, file, good)
            return

        else:
            self.log.info('Check ZERO/Broken file: OK ')

            #
            # TEST 2 Check SDS file:
            #
            # check if the filename reflect the infos inside mseed header

            fileinside = ''
            [xnet,xsta,xloc,xcha,xtype,xyear,xjday] = filename.split('.')
            ok = False

            if xtype not in self.config['TYPE_CODE']:
                self.log.error('Check SDS file: FAILED - Type-Code not allowed')
                print("ERROR Check SDS: Type-Code not allowed")
                self.error_code = "2"
                good = False                        
                self._move_file(filename, file, good)                        
                return

            try:
                for tr in st:
                    
                    realdata = tr.stats.network + '.' + tr.stats.station + '.' + tr.stats.location + '.' +\
                               tr.stats.channel + '.' + xtype + '.' + str(tr.stats.starttime.year) + '.' + str(tr.stats.starttime.julday).zfill(3)
                    #self.log.info(" realdata:  "+realdata)

                    if xnet == tr.stats.network and xsta == tr.stats.station and xloc == tr.stats.location and xcha == tr.stats.channel and xtype in self.config['TYPE_CODE'] and xyear == str(tr.stats.starttime.year) and xjday == str(tr.stats.starttime.julday).zfill(3):
                        ok = True
                    else:
                        ok = False
                        break

                self.log.info(" realdata:  "+realdata)        
                if ok:
                    self.log.info('Check SDS file: OK ')

                else:                        
                    self.log.error('Check SDS file: FAILED ')
                    print("ERROR Check SDS: info inside file " + realdata + " not equal to name " + filename)
                    self.error_code = "2"
                    good = False                        
                    self._move_file(filename, file, good)                        
                    return
                
            except Exception as e:
                self.log.error('2-1 unexpected error on step 2: ')
                self.log.error(e)
                print(e)
                self.error_code = "21"
                good = False
                self._move_file(filename, file, good)
                return

            #
            # TEST 3 check RATE filename (band code)
            #
            # check if sampling_rate of filename is consistent with mseed header

            try:
                if band_code[band_file_code][0] <= tr.stats.sampling_rate <= band_code[band_file_code][1]:
                    self.log.info("Check RATE file: OK ")
                else:
                    self.log.error("Check RATE file: FAILED ")
                    print("ERROR Check RATE band code: " + tr.stats.sampling_rate )
                    self.error_code = "3"
                    good = False
                    self._move_file(filename, file, good)
                    return

            except Exception as e:
                self.log.error('3-1 unexpected error on step 3: ')
                self.log.error(e)
                print(e)
                self.error_code = "31"
                good = False
                self._move_file(filename, file, good)
                return

            #
            # TEST 4 check EPOCH in file
            #
            # check if mseed data are consistent with station epoch
            
            netsta = [tr.stats.network + '.' + tr.stats.station]
            url_endpoint = self.config['STATION_ENDPOINT'] + 'level=channel&net=' + tr.stats.network + '&station=' + tr.stats.station + '&format=text'

            print(url_endpoint)

            epochs = self.utils.getEpochsFromService(netsta, url_endpoint)
            if len(epochs) == 0:
                print("epochs no-data")
                self.log.error("Check EPOCH file: FAILED: no-data ")
                good = False
                self.error_code = "41"
                self._move_file(filename, file, good)
                return

            # print(epochs)
            scnl = tr.stats.network + '.' + tr.stats.station + '.' + tr.stats.location + '.' + tr.stats.channel
            jday = str(tr.stats.starttime.year) + '.' + str(tr.stats.starttime.julday)
            # @TODO get better start and end date : first startdate last enddate
            self.session['SESSION']['STARTIME'] = str(tr.stats.starttime)
            self.session['SESSION']['ENDTIME'] = str(tr.stats.endtime)
            julday = datetime.datetime.strptime(jday, '%Y.%j').date()
            meta_OK = False
            for ep in epochs:
                [x, start, end] = ep
                if scnl == x:
                    if self.utils.time_in_range(start, end, julday):
                        meta_OK = True

            if meta_OK:
                self.log.info("Check EPOCH file: OK ")
            else:
                self.log.error("Check EPOCH file: FAILED: time not in range ")
                print("ERROR Check EPOCH " + jday)
                self.error_code = "4"
                good = False
                self._move_file(filename, file, good)
                return

        self._move_file(filename, file, good)
        return

    #
    # Final move file
    #
    def _move_file(self, filename, file, good):

        if self.config['NOT_MOVE'] == 'true':
            print(" no move : NOT_MOVE = true")
            return

        if not good :
            message = "SANITY-Checks has FAILED, moved file into BAD " + self.config['ARCHIVE_BAD'] + filename
            self.log.error(message)
            self.session['SESSION']['EXIT'] = 1

            # Goto or Exit ?
            if self.config['IF_BAD_GOTO'] != 'none':
                # jump to the GOTO action and keep the current file
                self.session['SESSION']['GOTO'] = self.config['IF_BAD_GOTO']
                self.session['SESSION']['IS_BAD'] = True
                self.session['SESSION']['BAD_ERROR'] = self.error_code
            else:
                # skip to the next file (exit=1) and move current file into BAD;
                # check if bad archive is SDS
                if self.config['BAD_SDS']:
                    target_path = self.utils.sdsPath(self.config['ARCHIVE_BAD'], os.path.basename(file))
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)
                    #self.session['SESSION']['EXIT'] = 1
                    shutil.move(file, target_path + "-" + self.error_code)
                                                           
                else:    
                    shutil.move(file, self.config['ARCHIVE_BAD'] + filename + "-" + self.error_code)
                    #self.session['SESSION']['EXIT'] = 1

                # remove if version
                if '#' in self.filevers:
                    os.remove(os.path.join( os.path.dirname(file), self.filevers))    
                    
        return
        