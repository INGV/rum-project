#!/usr/bin/env python3
# coding: utf-8

"""
HdfsDAO
-------
A small Data Access Object that exposes basic HDFS operations via WebHDFS
(using hdfs.ext.kerberos.KerberosClient) with Kerberos ticket check/renew
logic implemented as an internal method.

BEWARE: this DAO require docker image 'base-ruler:1.8'

Configuration (yaml):
<config>
    ....
    "HDFS":
        "WEBHDFS_URL": "http://master02.spark.int.ingv.it:14000"
    "KERBEROS":
        "KEYTAB": "/usr/src/code/project/spare/massimo.fares.keytab", s
        "PRINCIPAL": "massimo.fares@SPARK.INT.INGV.IT"

Prerequisites:
apt-get install krb5-config
apt-get install libkrb5-dev
apt-get install krb5-user
pip3 install requests_kerberos
pip3 install hdfs

Manual kinit:
klist -kt massimo.fares.keytab

Manual kinit:
env KRB5_TRACE=/dev/stderr  HADOOP_OPTS="$HADOOP_OPTS -Djava.security.krb5.conf=krb5.conf" KRB5_CONFIG=krb5.conf kinit -kt massimo.fares.keytab massimo.fares@SPARK.INT.INGV.IT -V

Usage:
    dao = HdfsDAO(config, logger)
    dao.connect()
    dao.list_dir("/path")
    dao.copy_local2hdfs_overwrite("/local/file", "/hdfs/target")
    dao.close()



# Module-Author:
    Massimo Fares, INGV - Italy <massimo.fares@ingv.it>

"""

from typing import Optional, Dict, Any, List
import re
import subprocess
from datetime import datetime, timedelta
import requests
from hdfs.ext.kerberos import KerberosClient
from requests_kerberos import HTTPKerberosAuth, DISABLED

"""
# Data Access Object for HDFS operations via WebHDFS + Kerberos.

Public methods:
  - connect(): establish session and Kerberos-authenticated KerberosClient
  - list_dir(path)
  - list_user_dir(path)
  - copy_local2hdfs_overwrite(local, hdfs)
  - copy_local2hdfs(local, hdfs)
  - move_hdfs2hdfs(src, dst)
  - copy_hdfs2local(hdfs, local)
  - delete_hdfs(hdfs_path)
  - close(): cleanup session
"""
class HdfsDAO:

    def __init__(self, config, log) -> None:
        """
        Initialize HdfsDAO.
        """
        self.log = log
        self.config = config

        # WebHDFS base URL from config
        self.webhdfs_url = self.config['HDFS']['WEBHDFS_URL']

        # Kerberos config
        self._keytab = self.config['KERBEROS']['KEYTAB'] #self.config.get("KERBEROS", {}).get("KEYTAB")
        self._principal = self.config['KERBEROS']['PRINCIPAL'] #self.config.get("KERBEROS", {}).get("PRINCIPAL")

        # Internal state
        self.kclient= None
        self.session = None
        self.token_is_live= False

    # ---------------------------
    # Public API
    # ---------------------------
    def connect(self, min_remaining_minutes: int = 60) -> bool:
        """
        Connect to HDFS using Kerberos. Ensures a valid Kerberos ticket
        before instantiating the KerberosClient.

        Returns
        -------
        bool
            True on successful connection, False otherwise (logs error).
        """
        try:
            # Ensure Kerberos ticket is valid/renewed
            self.token_is_live = self._check_and_renew_kerberos(min_remaining_minutes)
            if not self.token_is_live:
                self.log.error("Kerberos ticket invalid or could not be renewed. Aborting connect.")
                return False

            # Initialize HTTP session
            self.session = requests.Session()

            # Create KerberosClient for WebHDFS operations
            self.kclient = KerberosClient(
                url=self.webhdfs_url,
                root='/',
                mutual_auth='OPTIONAL',
                session=self.session
            )

            # Test connection by listing root; will raise if something fails
            self.kclient.list("/")

            self.log.info("Connected to HDFS (WebHDFS) successfully.")
            return True

        except Exception as exc:
            self.log.error(f"HDFS connection error: {exc}")
            # Keep token_is_live as False when connect fails
            self.token_is_live = False
            return False

    def list_dir(self, dirpath: str = '/user/massimo.fares') -> Optional[List[str]]:
        """
        List contents of an HDFS directory.

        Returns list of names or None on failure.
        """
        try:
            if not self.token_is_live:
                raise RuntimeError("Kerberos authentication is not active.")

            self.log.info(f"Listing HDFS directory: {dirpath}")
            return self.kclient.list(dirpath)
        except Exception as exc:
            self.log.error(f"Error in list_dir({dirpath}): {exc}")
            return None

    def list_user_dir(self, user_path: str = '/user/massimo.fares') -> Optional[List[str]]:
        """
        List a user's HDFS directory.
        """
        try:
            if not self.token_is_live:
                raise RuntimeError("Kerberos authentication is not active.")

            self.log.info(f"Listing user directory: {user_path}")
            return self.kclient.list(user_path)
        except Exception as exc:
            self.log.error(f"Error in list_user_dir({user_path}): {exc}")
            return None

    def hdfs_mkdir(self, hdfs_path: str) -> bool:
        """
        Create a directory in HDFS using WebHDFS and Kerberos authentication.

        Parameters
        ----------
        hdfs_path : str
            Full HDFS directory path (e.g. '/user/test/newdir').

        Returns
        -------
        bool
            True if created successfully or already exists, False on error.
        """

        # Build WebHDFS URL
        url = (
            f"{self.webhdfs_url}/webhdfs/v1/{quote(hdfs_path.lstrip('/'))}"
            "?op=MKDIRS&permission=755"
        )

        try:
            if not self.token_is_live:
                raise RuntimeError("Kerberos authentication is not active.")

            self.log.info(f"Creating hdfs directory: {hdfs_path}")
            response = self.session.put(url, timeout=10)

            if response.status_code == 200:
                self.log.info(f"HDFS directory created: {hdfs_path}")
                return True
            elif response.status_code == 401:
                self.log.error("Kerberos authentication failed.")
            else:
                self.log.error(
                    f"Error creating directory: {hdfs_path} "
                    f"(HTTP {response.status_code}) → {response.text}"
                )

        except Exception as e:
            self.log.error(f"Exception during mkdir on HDFS: {e}")

        return False

    def copy_local2hdfs_overwrite(self, source_path: str, target_path: str) -> bool:
        """
        Upload a local file to HDFS, overwriting target if it exists.

        Returns True on success, False on failure.
        """
        try:
            if not self.token_is_live:
                raise RuntimeError("Kerberos authentication is not active.")

            self.log.info(f"Uploading (overwrite) {source_path} -> {target_path}")
            self.kclient.upload(
                hdfs_path=target_path,
                local_path=source_path,
                overwrite=True,
                n_threads=1
            )
            return True
        except Exception as exc:
            self.log.error(f"Upload (overwrite) error: {exc}")
            return False

    def copy_local2hdfs(self, source_path: str, target_path: str) -> bool:
        """
        Upload a local file to HDFS without overwriting (fails if exists).
        """
        try:
            if not self.token_is_live:
                raise RuntimeError("Kerberos authentication is not active.")

            self.log.info(f"Uploading {source_path} -> {target_path} (no overwrite)")
            self.kclient.upload(
                hdfs_path=target_path,
                local_path=source_path,
                overwrite=False,
                n_threads=1
            )
            return True
        except Exception as exc:
            self.log.error(f"Upload (no overwrite) error: {exc}")
            return False

    def move_hdfs2hdfs(self, hdfs_src_path: str, hdfs_dst_path: str) -> bool:
        """
        Rename or move a file within HDFS.
        """
        try:
            if not self.token_is_live:
                raise RuntimeError("Kerberos authentication is not active.")

            self.log.info(f"Renaming/moving: {hdfs_src_path} -> {hdfs_dst_path}")
            self.kclient.rename(hdfs_src_path, hdfs_dst_path)
            return True
        except Exception as exc:
            self.log.error(f"Move error: {exc}")
            return False

    def copy_hdfs2local(self, source_hdfs_path: str, target_local_path: str = '.') -> bool:
        """
        Download an HDFS file to local filesystem.
        """
        try:
            if not self.token_is_live:
                raise RuntimeError("Kerberos authentication is not active.")

            self.log.info(f"Downloading {source_hdfs_path} -> {target_local_path}")
            self.kclient.download(
                hdfs_path=source_hdfs_path,
                local_path=target_local_path,
                overwrite=True
            )
            return True
        except Exception as exc:
            self.log.error(f"Download error: {exc}")
            return False

    def delete_hdfs(self, hdfs_dst_path: str) -> bool:
        """
        Delete a file on HDFS (non-recursive by default).
        """
        try:
            if not self.token_is_live:
                raise RuntimeError("Kerberos authentication is not active.")

            self.log.info(f"Deleting HDFS path: {hdfs_dst_path}")
            self.kclient.delete(hdfs_dst_path, recursive=False, skip_trash=False)
            return True
        except Exception as exc:
            self.log.error(f"Delete error for {hdfs_dst_path}: {exc}")
            return False

    def close(self) -> None:
        """
        Close HTTP session and clear Kerberos client reference.
        """
        try:
            if self.session:
                self.session.close()
            self.kclient = None
            self.token_is_live = False
            self.log.info("HdfsDAO session closed.")
        except Exception as exc:
            self.log.debug(f"Error during close(): {exc}")

    # ---------------------------
    # Private helpers
    # ---------------------------
    def _check_and_renew_kerberos(self, min_remaining_minutes: int = 60) -> bool:
        """
        Check current Kerberos ticket expiration and try to renew it if needed.

        Steps:
          1) run `klist` to read ticket information
          2) parse expiry date and compare with now
          3) try `kinit -R` to renew
          4) if renew fails and keytab/principal configured try `kinit -kt keytab principal`

        Returns True if ticket is currently valid or was successfully renewed.
        """
        try:
            self.log.info("Checking Kerberos ticket status using klist...")
            result = subprocess.run(["klist"], capture_output=True, text=True)

            if result.returncode != 0:
                self.log.error("Unable to read Kerberos ticket using klist.")
                self.log.debug(f"klist stderr: {result.stderr}")
                return False

            output = result.stdout or result.stderr or ""
            self.log.debug(f"klist output:\n{output}")

            expire_dt = self._parse_klist_expiry(output)
            if expire_dt is None:
                self.log.error("Could not parse Kerberos ticket expiration time from klist output.")
                return False

            now = datetime.now()
            remaining = expire_dt - now
            self.log.info(f"Kerberos ticket expires in: {remaining}")

            if remaining > timedelta(minutes=min_remaining_minutes):
                self.log.info("Kerberos ticket is valid — no renewal needed.")
                return True

            self.log.warning(f"Kerberos ticket expiring soon (in {remaining}). Attempting kinit -R...")

            # Attempt to renew existing ticket
            renew = subprocess.run(["kinit", "-R"], capture_output=True, text=True)
            if renew.returncode == 0:
                self.log.info("Kerberos ticket successfully renewed with kinit -R.")
                return True

            self.log.warning("kinit -R failed. Attempting full kinit using keytab configured...")

            # Attempt full kinit with keytab/principal
            if self._keytab and self._principal:
                self.log.info(f"Attempting full kinit with keytab {self._keytab} and principal {self._principal}.")
                full = subprocess.run(
                    ["kinit", "-kt", self._keytab, self._principal],
                    capture_output=True,
                    text=True
                )  # missed '-V' last option by hand
                if full.returncode == 0:
                    self.log.info("Kerberos ticket successfully renewed via keytab.")
                    return True
                else:
                    self.log.error("Full kinit via keytab failed.")
                    self.log.debug(f"kinit stderr: {full.stderr}")
                    return False

            self.log.error("Problem w keytab/principal and kinit -R failed — manual kinit required.")
            return False

        except Exception as exc:
            self.log.error(f"Unexpected error in _check_and_renew_kerberos(): {exc}")
            return False

    def _parse_klist_expiry(self, klist_output: str) -> Optional[datetime]:
        """
        Parse klist output to extract the ticket expiration datetime.

        This function tries a couple of common formats that appear in klist output,
        and returns a datetime object in local time if successful, otherwise None.
        """
        # Normalize whitespace
        txt = klist_output.replace("\r", "\n")

        # Try to find explicit date/time patterns like:
        #  - "Expires: 10/04/2025 13:45:00"
        #  - "End Time: 10/04/2025 13:45:00"
        #  - or lines with date/time like "10/04/2025 13:45:00"
        pattern = re.compile(r"(\d{1,2}/\d{1,2}/\d{2,4}\s+\d{1,2}:\d{2}(?::\d{2})?)")
        m = pattern.search(txt)
        if not m:
            # As fallback, search for ISO-like date strings
            iso_pattern = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})")
            m2 = iso_pattern.search(txt)
            if not m2:
                self.log.debug("No recognizable date/time string in klist output.")
                return None
            expire_str = m2.group(1)
            try:
                return datetime.strptime(expire_str, "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return None

        expire_str = m.group(1).strip()
        # Try parsing with century (yyyy) first, then without
        for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%y %H:%M"):
            try:
                return datetime.strptime(expire_str, fmt)
            except Exception:
                continue

        # As a last resort return None
        self.log.debug(f"Could not parse date string '{expire_str}' with common formats.")
        return None
