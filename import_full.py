import listenbrainz_spark
import listenbrainz_spark.config as config
import shutil
import sys
import tempfile
import time

from listenbrainz_spark import hdfs_connection
from listenbrainz_spark.data.import_data import import_dump
from listenbrainz_spark.data.import_data.ftp import ListenBrainzFTPDownloader

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: import.py <app_name>")
        sys.exit(-1)

    listenbrainz_spark.init_spark_session(sys.argv[1])
    hdfs_connection.init_hdfs(config.HDFS_HTTP_URI)
    temp_dir = tempfile.mkdtemp()
    t0 = time.time()
    print("Downloading dump...")
    file_path = ListenBrainzFTPDownloader().download_full_dump(temp_dir)
    print("Dump downloaded in %.2f seconds!" % (time.time() - t0))
    import_dump.full(file_path)
    shutil.rmtree(temp_dir)
