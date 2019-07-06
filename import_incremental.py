import listenbrainz_spark
import listenbrainz_spark.config as config
import sys

from listenbrainz_spark import hdfs_connection
from listenbrainz_spark.data.import_data import import_dump

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: import_incremental.py <app_name>")
        sys.exit(-1)

    listenbrainz_spark.init_spark_session(sys.argv[1])
    hdfs_connection.init_hdfs(config.HDFS_HTTP_URI)
    import_dump.incremental(file_path)

