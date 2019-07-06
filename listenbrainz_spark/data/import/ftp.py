import ftplib
import logging
import tempdir


class ListenBrainzFTPDownloader:

    def __init__(self):
        self.connect()

    def connect(self):
        self.connection = ftplib.FTP('ftp.musicbrainz.org') # TODO: get this from config
        self.connection.login() # TODO: catch ftplib.error_perm
        self.connection.cwd('pub/musicbrainz/listenbrainz') #TODO: this should probably be a constant up top

    def list_dir(self, path=None, verbose=False):
        files = []
        def callback(x):
            files.append(x)

        cmd = 'LIST' if verbose else 'NLST'
        if path:
            cmd += ' ' + path
        self.connection.retrlines(cmd, callback=callback)
        return files

    def download_file_binary(src, dest):
        with open(dest, 'wb') as f:
            self.connection.retrbinary('RETR %s' % src, f.write)
        return dest


    def get_spark_dump_name(self, dump_name):
        _, _, dump_id, date, hour, _ = dump_name.split('-')
        return 'listenbrainz-listens-dump-%s-%s-%s-spark-full.tar.xz' % (dump_id, date, hour)

    def download_full_dump(self, directory, dump_id=None):
        self.connection.cwd('fullexport')
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
        spark_dump_file_name = self.get_spark_dump_name(req_dump)
        logging.info("Downloading %s...", spark_dump_file_name)
        f = self.download_file_binary(spark_dump_file_name, os.path.join(directory, spark_dump_file_name))
        logging.info("Done!")
        return f


if __name__ == '__main__':
    f = ListenBrainzFTPDownloader().download_full_dump(tempdir.mkdtemp())
    print(f)
