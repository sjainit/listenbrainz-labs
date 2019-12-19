#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from listenbrainz_spark.stats.user import utils as u1
from listenbrainz_spark import utils
from pyspark.sql import Row
from listenbrainz_spark.tests import SparkTestCase

class UtilsTestCase(SparkTestCase):
    
    def test_get_artists(self):
        df = utils.create_dataframe(Row(user_name='user2',artist_name='artist1', artist_msid='1',artist_mbids='1'),schema=None)
        df1 = utils.create_dataframe(Row(user_name='user1',artist_name='artist1', artist_msid='1',artist_mbids='1'),schema=None)
        df = df.union(df1)
        df2 = utils.create_dataframe(Row(user_name='user2',artist_name='artist1', artist_msid='1',artist_mbids='1'),schema=None)
        df = df.union(df2)
        df.createTempView('table')
        dictionary = {
                    'user2': [{
                        'artist_name': 'artist1',
                        'artist_msid': '1',
                        'artist_mbids': '1',
                        'listen_count': 2  
                    }],
                        'user1': [{
                        'artist_name': 'artist1',
                        'artist_msid': '1',
                        'artist_mbids': '1',
                        'listen_count': 1  
                    }]
                  }
        self.assertEqual(u1.get_artists('table'), dictionary)
        self.assertGreater(u1.get_artists('table')['user2'][0]['listen_count'], u1.get_artists('table')['user1'][0]['listen_count'])
        self.assertEqual(list(u1.get_artists('table'))[0], 'user2')
    
    def test_get_recordings(self):
        df = utils.create_dataframe(Row(user_name='user2',artist_name='artist1', artist_msid='1',artist_mbids='1',track_name='test', recording_msid='1', recording_mbid='1', release_name='test',release_msid='1', release_mbid='1'), schema=None)
        df1 = utils.create_dataframe(Row(user_name='user1',artist_name='artist1', artist_msid='1',artist_mbids='1',track_name='test', recording_msid='1', recording_mbid='1', release_name='test',release_msid='1', release_mbid='1'), schema=None)
        df = df.union(df1)
        df2 = utils.create_dataframe(Row(user_name='user1',artist_name='artist1', artist_msid='1',artist_mbids='1',track_name='test', recording_msid='1', recording_mbid='1', release_name='test',release_msid='1', release_mbid='1'), schema=None)
        df = df.union(df2)
        df.createOrReplaceTempView("table")
        dictionary =  {
                    'user1' : [{
                        'track_name': 'test',
                        'recording_msid': '1',
                        'recording_mbid': '1',
                        'artist_name': 'artist1',
                        'artist_msid': '1',
                        'artist_mbids': '1',
                        'release_name': 'test',
                        'release_msid': '1',
                        'release_mbid': '1',
                        'listen_count': 2
                    }],
                    'user2' : [{
                        'track_name': 'test',
                        'recording_msid': '1',
                        'recording_mbid': '1',
                        'artist_name': 'artist1',
                        'artist_msid': '1',
                        'artist_mbids': '1',
                        'release_name': 'test',
                        'release_msid': '1',
                        'release_mbid': '1',
                        'listen_count': 1
                    }]
                }
                    
        self.assertEqual(u1.get_recordings('table'), dictionary)
        self.assertGreater(u1.get_recordings('table')['user1'][0]['listen_count'], u1.get_recordings('table')['user2'][0]['listen_count'])
        self.assertEqual(list(u1.get_recordings('table'))[0], 'user1')
    
    def test_get_releases(self):
        df = utils.create_dataframe(Row(user_name='user2',artist_name='artist1', artist_msid='1',artist_mbids='1',release_name='test', release_msid='1', release_mbid='1'), schema=None)
        df1 = utils.create_dataframe(Row(user_name='user1',artist_name='artist1', artist_msid='1',artist_mbids='1',release_name='test', release_msid='1', release_mbid='1'), schema=None)
        df = df.union(df1)
        df2 = utils.create_dataframe(Row(user_name='user1',artist_name='artist1', artist_msid='1',artist_mbids='1',release_name='test', release_msid='1', release_mbid='1'), schema=None)
        df = df.union(df2)
        df.createOrReplaceTempView("table")
        dictionary =  {
                    'user1' : [{
                        'artist_name': 'artist1',
                        'artist_msid': '1',
                        'artist_mbids': '1',
                        'release_name': 'test',
                        'release_msid': '1',
                        'release_mbid': '1',
                        'listen_count': 2
                    }],
                    'user2' : [{
                        'artist_name': 'artist1',
                        'artist_msid': '1',
                        'artist_mbids': '1',
                        'release_name': 'test',
                        'release_msid': '1',
                        'release_mbid': '1',
                        'listen_count': 1
                    }]
                }
        self.assertEqual(u1.get_releases('table'), dictionary)
        self.assertGreater(u1.get_releases('table')['user1'][0]['listen_count'], u1.get_releases('table')['user2'][0]['listen_count'])
        self.assertEqual(list(u1.get_releases('table'))[0], 'user1')