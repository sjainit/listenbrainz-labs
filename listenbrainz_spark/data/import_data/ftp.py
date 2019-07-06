import ftplib
import logging
import os
import requests
import tempfile


class ListenBrainzFTPDownloader:

    def __init__(self):
        self.connect()

    def connect(self):
        self.connection = ftplib.FTP('ftp.musicbrainz.org') # TODO: get this from config
        self.connection.login() # TODO: catch ftplib.error_perm

    def list_dir(self, path=None, verbose=False):
        files = []
        def callback(x):
            files.append(x)

        cmd = 'LIST' if verbose else 'NLST'
        if path:
            cmd += ' ' + path
        self.connection.retrlines(cmd, callback=callback)
        return files

    def download_file_binary(self, src, dest):
        with open(dest, 'wb') as f:
            self.connection.retrbinary('RETR %s' % src, f.write)
        return dest


    def get_spark_dump_name(self, dump_name, full=True):
        _, _, dump_id, date, hour, _ = dump_name.split('-')
        dump_type = 'full' if full else 'incremental'
        return 'listenbrainz-listens-dump-%s-%s-%s-spark-%s.tar.xz' % (dump_id, date, hour, dump_type)

    def get_dump_dir_name(self, dump_id, full=True):
        r = requests.get('https://beta-api.listenbrainz.org/1/status/get-dump-info', params={'id': dump_id})
        print(r.text)
        timestamp = r.json()['timestamp']
        dump_type = 'full' if full else 'incremental'
        return 'listenbrainz-dump-%d-%s-%s' % (dump_id, timestamp, dump_type)

    def download_full_dump(self, directory, dump_id=None):
        self.connection.cwd('/pub/musicbrainz/listenbrainz/fullexport')
        full_dumps = self.list_dir()
        if dump_id:
            for dump_name in full_dumps:
                if int(dump_name.split('-')[2]) == dump_id:
                    req_dump = dump_name
                    break
            else:
                logging.critical("Could not find full dump with ID: %d, exiting", dump_id)
                raise SystemExit("Could not find full dump with ID: %d, exiting" % dump_id)
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
        self.connection.cwd('/pub/musicbrainz/listenbrainz/incremental')

        incremental_dumps = self.list_dir()
        logging.info("List of available incremental dumps: ")
        for dump_name in incremental_dumps:
            logging.info(dump_name)


        dump_dir = self.get_dump_dir_name(dump_id=dump_id, full=False)
        print(dump_dir)
        dump_name = self.get_spark_dump_name(dump_dir, full=False)
        print("Downloading %s..." % dump_name)
        f = self.download_file_binary(os.path.join(dump_dir, dump_name), os.path.join(directory, dump_name))
        print("Done!")
        self.connection.cwd("/")
        return f


if __name__ == '__main__':
    f = ListenBrainzFTPDownloader().download_incremental_dump(tempfile.mkdtemp(), dump_id=60)
    print(f)
