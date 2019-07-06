import ftplib
import logging
import os
import requests
import tempfile

from listenbrainz_spark import config

class DumpNotFoundException(Exception):
    pass


class ListenBrainzFTPDownloader:

    def __init__(self):
        self.connect()

    def connect(self):
        try:
            self.connection = ftplib.FTP(config.FTP_SERVER_URI)
            self.connection.login()
        except ftplib.error_perm:
            logging.critical("Couldn't connect to FTP Server, try again...")
            raise SystemExit

    def list_dir(self, path=None, verbose=False):
        """ Lists the current directory

        Args:
            path (str): the directory to list, lists the current working dir if not provided
            verbose (bool): whether to return file properties or just file names

        Returns:
            [str]: a list of contents of the directory
        """
        files = []
        def callback(x):
            files.append(x)

        cmd = 'LIST' if verbose else 'NLST'
        if path:
            cmd += ' ' + path
        self.connection.retrlines(cmd, callback=callback)
        return files

    def download_file_binary(self, src, dest):
        """ Download file `src` from the FTP server to `dest`
        """
        with open(dest, 'wb') as f:
            try:
                self.connection.retrbinary('RETR %s' % src, f.write)
            except ftplib.error_perm as e:
                logging.critical("Could not download file: %s", str(e))
                raise DumpNotFoundException
        return dest


    def get_spark_dump_name(self, dump_name, full=True):
        """ Get the name of the spark dump archive from the dump directory name
        """
        _, _, dump_id, date, hour, _ = dump_name.split('-')
        dump_type = 'full' if full else 'incremental'
        return 'listenbrainz-listens-dump-%s-%s-%s-spark-%s.tar.xz' % (dump_id, date, hour, dump_type)

    def get_dump_dir_name(self, dump_id, full=True):
        """ Get dump directory name from the dump ID
        """
        r = requests.get(config.LISTENBRAINZ_API_URI + '/1/status/get-dump-info', params={'id': dump_id})
        if r.status_code == 404:
            logging.critical("No dump exists with ID: %d", dump_id)
            raise DumpNotFoundException("No dump exists with ID: %d" % dump_id)
        timestamp = r.json()['timestamp']
        dump_type = 'full' if full else 'incremental'
        return 'listenbrainz-dump-%d-%s-%s' % (dump_id, timestamp, dump_type)

    def download_full_dump(self, directory, dump_id=None):
        """ Download a ListenBrainz full spark dump to the specified directory

        Args:
            directory (str): the path to which the dump should be downloaded
            dump_id (int): the ID of the dump to be downloaded, downloads the latest dump if not specified

        Returns:
            the path to the downloaded ListenBrainz spark dump
        """
        self.connection.cwd('/pub/musicbrainz/listenbrainz/fullexport')
        full_dumps = self.list_dir()
        if dump_id:
            for dump_name in full_dumps:
                if int(dump_name.split('-')[2]) == dump_id:
                    req_dump = dump_name
                    break
            else:
                logging.critical("Could not find full dump with ID: %d, exiting", dump_id)
                raise DumpNotFoundException("Could not find full dump with ID: %d, exiting" % dump_id)
        else:
            req_dump = full_dumps[-1]

        self.connection.cwd(req_dump)
        spark_dump_file_name = self.get_spark_dump_name(req_dump, full=True)
        print("Downloading %s..." % spark_dump_file_name)
        f = self.download_file_binary(spark_dump_file_name, os.path.join(directory, spark_dump_file_name))
        print("Done!")
        self.connection.cwd("/")
        return f

    def download_incremental_dump(self, directory, dump_id):
        """ Download a ListenBrainz incremental spark dump to the specified directory

        Args:
            directory (str): the path where the dump should be downloaded
            dump_id (int): the ID of the dump to be downloaded (required)

        Returns:
            the path to the downloaded incremental dump
        """
        self.connection.cwd('/pub/musicbrainz/listenbrainz/incremental')
        dump_dir = self.get_dump_dir_name(dump_id=dump_id, full=False)
        dump_name = self.get_spark_dump_name(dump_dir, full=False)
        print("Downloading %s..." % dump_name)
        f = self.download_file_binary(os.path.join(dump_dir, dump_name), os.path.join(directory, dump_name))
        print("Done!")
        self.connection.cwd("/")
        return f
