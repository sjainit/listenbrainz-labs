import unittest
import uuid
import listenbrainz_spark

class SparkTestCase(unittest.TestCase):

    def setUp(self):
        listenbrainz_spark.init_test_session('spark-test-run-{}'.format(str(uuid.uuid4())))
