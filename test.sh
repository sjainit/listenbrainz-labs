#!/bin/bash
docker stop listenbrainz-spark-test-$USER
docker rm listenbrainz-spark-test-$USER
docker run \
    -v `pwd`:/rec \
    --name listenbrainz-spark-test \
    metabrainz/listenbrainz-spark:latest \
    py.test
docker stop listenbrainz-spark-test-$USER
docker rm listenbrainz-spark-test-$USER
