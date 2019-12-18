#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 12:19:57 2019

@author: sarthak
"""
#
#from listenbrainz_spark.stats.user import utils as u1
#from listenbrainz_spark import utils
#from pyspark.sql import Row
#from listenbrainz_spark.tests import SparkTestCase
#
#class UtilsTestCase(SparkTestCase):
#    
#    def test_get_artists(self):
#        df=utils.create_dataframe(Row(user_name='user1', artist_name='artist1',artist_msid='1',artist_mbids='1'),schema=None)
#        
#        dictionary={
#                    'user1': [{
#                        'artist_name': 'artist1',
#                        'artist_msid': '1',
#                        'artist_mbids': '1',
#                        'listen_count': 1
#                    }]}
#        self.assertEqual(u1.get_artists(),dictionary)
