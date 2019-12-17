	#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 15 20:12:56 2019

@author: sarthak
"""

import datetime
import listenbrainz_spark
from listenbrainz_spark import utils, hdfs_connection, config
from pyspark.sql import Row
from listenbrainz_spark import stats
from listenbrainz_spark.tests import SparkTestCase


class InitTestCase(SparkTestCase):

    def test_replace_days(self):
        self.assertEqual(stats.replace_days(datetime.datetime(2019,5,12),13),datetime.datetime(2019,5,13,0,0))
        
    def test_adjust_months(self):
        self.assertEqual(stats.adjust_months(datetime.datetime(2019,5,12),3,True),datetime.datetime(2019,2,12,0,0))
        
    def test_ajust_days(self):
        self.assertEqual(stats.adjust_days(datetime.datetime(2019,5,12),3,True),datetime.datetime(2019,5,9,0,0))
        
    def test_run_query(self):
        df = utils.create_dataframe(Row(column1=1, column2=2), schema=None)
        utils.register_dataframe(df,'df')
        new_df=stats.run_query("""
      SELECT *
        FROM df
     """)
        self.assertEqual(new_df.count(),df.count())
        
        
        
