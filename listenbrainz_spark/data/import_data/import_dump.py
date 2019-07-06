import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import time
import hdfs

import listenbrainz_spark
import listenbrainz_spark.config as config

from listenbrainz_spark.utils import create_path

from datetime import datetime
from hdfs.util import HdfsError
from listenbrainz_spark import hdfs_connection
from listenbrainz_spark.constants import LAST_FM_FOUNDING_YEAR
from listenbrainz_spark.schema import convert_listen_to_row, listen_schema, convert_to_spark_json
import pyspark.sql.functions as sql_functions

FORCE = True

def _is_json_file(filename):
    """ Check if passed filename is a file which contains listens

    Args:
        filename (str): the name of the file

    Returns:
        bool: True if the file contains listens, False otherwise
    """
    return filename.endswith('.json')


def _process_json_file(filename, data_dir, hdfs_path, full=True):
    """ Process a file containing listens from the ListenBrainz dump and add listens to
    appropriate dataframes.
    """
    start_time = time.time()
    file_df = listenbrainz_spark.session.read.json(config.HDFS_CLUSTER_URI + hdfs_path, schema=listen_schema).cache()
    print("Processing %d listens..." % file_df.count())

    if filename.split('/')[-1] == 'invalid.json':
        dest_path = os.path.join(data_dir, 'invalid.parquet')
    else:
        year = filename.split('/')[-2]
        month = filename.split('/')[-1][0:-5]
        dest_path = os.path.join(data_dir, year, '{}.parquet'.format(str(month)))

    print("Uploading to %s..." % dest_path)
    if full:
        file_df.write.format('parquet').save(config.HDFS_CLUSTER_URI + dest_path)
    else:
        file_df.write.mode('append').parquet(config.HDFS_CLUSTER_URI + dest_path)
    print("File processed in %.2f seconds!" % (time.time() - start_time))


def copy_to_hdfs(archive, full=True, threads=8):

    """ Create Spark Dataframes from a listens dump and save it to HDFS.

    Args:
        archive (str): the path to the listens dump
        threads (int): the number of threads to use for decompression of the archive
    """
    tmp_dump_dir = tempfile.mkdtemp()
    pxz_command = ['pxz', '--decompress', '--stdout', archive, '-T{}'.format(threads)]
    pxz = subprocess.Popen(pxz_command, stdout=subprocess.PIPE)
    destination_path = os.path.join('/', 'data', 'listenbrainz')
    if full and FORCE:
        print('Removing data directory if present...')
        hdfs_connection.client.delete(destination_path, recursive=True)
        print('Done!')

    dump_id = int(os.path.split(archive)[1].split('-')[2])
    if not full:
        hdfs_connection.client.download(os.path.join(destination_path, 'DATA_VERSION'), tmp_dump_dir)
        with open(os.path.join(tmp_dump_dir, 'DATA_VERSION')) as f:
            prev_dump_id = int(f.read().strip())

        if dump_id != prev_dump_id + 1:
            print("Incorrect incremental dump being imported, expected %d, got %d, exiting..." % prev_dump_id + 1, dump_id)
            raise SystemExit("Incorrect incremental dump being imported")

    file_count = 0
    total_time = 0.0
    with tarfile.open(fileobj=pxz.stdout, mode='r|') as tar:
        for member in tar:
            if member.isfile() and _is_json_file(member.name):
                print('Loading %s...' % member.name)
                t = time.time()
                tar.extract(member)
                tmp_hdfs_path = os.path.join(tmp_dump_dir, member.name)
                hdfs_connection.client.upload(hdfs_path=tmp_hdfs_path, local_path=member.name)
                _process_json_file(member.name, destination_path, tmp_hdfs_path, full=full)
                hdfs_connection.client.delete(tmp_hdfs_path)
                os.remove(member.name)
                file_count += 1
                time_taken = time.time() - t
                print("Done! Processed %d files. Current file done in %.2f sec" % (file_count, time_taken))
                total_time += time_taken
                average_time = total_time / file_count
                print("Total time: %.2f, average time: %.2f" % (total_time, average_time))

    with open(os.path.join(tmp_dump_dir, 'DATA_VERSION'), 'w') as f:
        f.write(str(dump_id) + "\n")
    hdfs_connection.client.upload(
        hdfs_path=os.path.join(destination_path, 'DATA_VERSION'),
        local_path=os.path.join(tmp_dump_dir, 'DATA_VERSION'),
        overwrite=True,
    )

    hdfs_connection.client.delete(tmp_dump_dir, recursive=True)
    shutil.rmtree(tmp_dump_dir)


def main(archive, full=True):
    print('Copying extracted dump to HDFS...')
    copy_to_hdfs(archive)
    print('Done!')
