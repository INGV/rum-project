#!/usr/bin/env python3
# coding: utf-8

"""
copy2hdfs - Simple action class to move (upload) files to HDFS.

# Disclaimer:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    any later version.
    This script is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY.

# Copyright:
    2025 Massimo Fares, INGV - Italy <massimo.fares@ingv.it>;
    EIDA Italia Team, INGV - Italy  <adaisacd.ont@ingv.it>

# License:
    GPLv3

# Platform:
    Linux / Python 3.x

# Action-Author:
    Massimo Fares, INGV - Italy <massimo.fares@ingv.it>

# Action-Description:
    Move-to-HDFS
    This Action moves a given file into the HDFS Archive or Warning Archive.

    FUTURE:
        Handles versioned files (indicated by '#') and tagged files (e.g., '.quarantine').
        Supports moving or copying files according to configuration.

    If there are errors during the move, they are logged and handled gracefully.


# CONFIG expected structure:

config = {
    "HDFS": {
        "WEBHDFS_URL": "http://namenode:port",
        "DEST_PATH": "/user/foo/destination_dir"   # HDFS destination folder
    },
    "KERBEROS": {
        "KEYTAB": "/path/to/keytab",
        "PRINCIPAL": "user@REALM"
    }
}

The class receives (from outside):
    - log:     logging object
    - config:  dictionary containing at least config["HDFS"]["DEST_PATH"]
    - session: a dictionary-like object (optional use)
"""

# in rum:
# from project.modules.hdfsmanager import HdfsDAO
from hdfsmanager import HdfsDAO


class copy2hdfs:
    """
    Class used to move local files into HDFS, using an existing HdfsDAO client.
    """

    def __init__(self, log, config, session):

        # in rum
        # self.config = config['ACTIONS_CONFIG']['MOVE2HDFS']
        self.config = config
        self.log = log
        self.session = session

        # Initialize the HDFS DAO client
        self.hdfs = HdfsDAO(self.config, self.log)
        # try to HDFS connect
        if not self.hdfs.connect():
            self.log.error("Unable to connect to HDFS.")
            raise RuntimeError("HDFS connection error")

    # ---------------------------------------------------------
    # upload to hdfs
    # ---------------------------------------------------------
    def _upload_file(self, source_path, dest_path):
        """
        Private method that performs the actual HDFS upload.

        Parameters
        ----------
        source_path : str
            Local path of the file to upload.
        dest_path : str
            HDFS destination directory (from config).
        """
        try:
            self.log.info(f"Uploading to HDFS: {source_path} --> {dest_path}")

            # Call the HDFS DAO method
            result = self.hdfs.copy_local2hdfs_overwrite(source_path, dest_path)

            if not result:
                self.log.error("HDFS upload failed.")
                return None

            self.log.info("File successfully uploaded to HDFS.")
            return result

        except Exception as e:
            self.log.error(f"Exception while uploading to HDFS: {e}")
            return False

    #
    # Action: do a copy to hdfs
    #
    def do_copy2hdfs(self, file):
        """
        Public method that reads DEST_PATH from config and uploads the file.

        Parameters
        ----------
        source_path : str
            Local path of the file to upload.

        Returns
        -------
        HDFS listing or None
        """
        source_path = file
        dest_path = self.config["HDFS"]["DEST_PATH"]

        if not dest_path:
            self.log.error("DEST_PATH missing in configuration.")
            return None

        copied = self._upload_file(source_path, dest_path)
        self.hdfs.close()

        return copied
