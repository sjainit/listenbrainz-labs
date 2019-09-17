import subprocess


def spark_submit(options):
    r = subprocess.run(['./py-spark-submit.sh', 'manage.py', 'user'], stdout=subprocess.PIPE)
    print(r.stdout)

if __name__ == '__main__':
    spark_submit(None)
