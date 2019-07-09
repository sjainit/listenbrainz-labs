import listenbrainz_spark
import os
import sys

from listenbrainz_spark.data import DATA_ROOT_PATH

def main():
    listenbrainz_spark.init_spark_session(app_name=sys.argv[1])
    df = listenbrainz_spark.sql_context.read.parquet(
        os.path.join(config.HDFS_CLUSTER_URI, DATA_ROOT_PATH, 'invalid.parquet')
    )

    for year in range(2002, 2020):
        for month in range(1, 13):
            try:
                print(year, '\t', month)
                file_df = listenbrainz_spark.sql_context.read.parquet(
                        os.path.join(config.HDFS_CLUSTER_URI, DATA_ROOT_PATH, str(year), '%d.parquet' % month)
                )
                df = df.union(file_df)
                print("Done!")
            except Exception:
                print("Doesn't exist!")
                pass
    print(df.count())

if __name__ == '__main__':
    main()
