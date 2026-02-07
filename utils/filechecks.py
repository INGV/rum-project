#! /usr/bin/env python3
"""
#
#  massimo.fares@ingv.it
#  adaisacd.ont@ingv.it
#
#  file utilities
#
"""

import urllib.request
import datetime
import csv
from io import StringIO
import os
import shutil
from pathlib import Path

#
# class that contains utility for filechecks
#
class filechecks_util():

    def __init__(self):
        self.this = 1
        # empty

    #
    # return the sds path of file
    """
    def sdsPath(self, root_dir, myfile):

        mypath = myfile.split(".")
        return root_dir + mypath[5] + "/" + mypath[0] + "/" + mypath[1] + "/" + mypath[3] + ".D/" + myfile
    """
    def sds_path(self, archive_dir: Path, file_name: str) -> Path:
        parts = file_name.split(".")
        if len(parts) < 6:
            raise ValueError(f"Invalid SDS filename: {file_name}")

        return (
                archive_dir
                / parts[5]
                / parts[0]
                / parts[1]
                / f"{parts[3]}.D"
                / file_name
        )

    #
    # Check if date is in range
    #
    def time_in_range(self, start, end, t):
        """Return true if t is in the range [start, end]"""
        if start <= end:
            return start <= t <= end
        else:
            return start <= t or t <= end

    #
    # Return epoch from webservices
    #
    def getEpochsFromService(self, netsta, url_endpoint):
        """Get epochs from station service for all net.sta in netsta"""
        eps = []
        for ns in netsta:
            net = ns.split('.')[0]
            sta = ns.split('.')[1]
            url = url_endpoint
            resource = urllib.request.urlopen(url)
            response = resource.read().decode(resource.headers.get_content_charset())
            f = StringIO(response)
            cr = csv.reader(f, delimiter='|')
            for row in cr:
                x = row[0]
                if x != "#Network ":
                    if row[-1] == "":
                        row[-1] = "2100-01-01T00:00:00"
                    xx = row[-2].split("T")
                    start = datetime.datetime.strptime(xx[0], '%Y-%m-%d').date()
                    xx = row[-1].split("T")
                    end = datetime.datetime.strptime(xx[0], '%Y-%m-%d').date()
                    x = x + "." + row[1] + "." + row[2] + "." + row[3]
                    eps.append([x, start, end])
        return eps


    #
    # reject file
    #
    def reject_file(self, filename, file, archive, sds=False, error_code='none'):

        if error_code == 'none' :
            error_code = "-rejected"
        

        if sds:
            try:
                target_path = self.sdsPath(archive, os.path.basename(file))
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                shutil.move(file, target_path + error_code)
            except Exception as e:                
                return e                  
            
        else:
            try:    
                shutil.move(file, archive + filename + error_code)
            except Exception as e:                
                return e 

        return 0

    #
    # delete file
    #
    def delete_file(self, file):


        try:
            os.remove(file)
        except Exception as e:                
            return e                  
            

        return 0