import subprocess


def spark_submit(options):
    subprocess.call('./py-spark-submit.sh', 'user')


if __name__ == '__main__':
    spark_submit(None)
