import subprocess


def spark_submit(options):
    r = subprocess.call(['./py-spark-submit.sh', 'manage.py', 'user'], capture_output=True)
    print(r.stdout)



if __name__ == '__main__':
    spark_submit(None)
