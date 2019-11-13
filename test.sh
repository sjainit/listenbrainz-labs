#!/bin/bash
docker run \
    -v `pwd`:/rec \
    --name listenbrainz-spark-test-$USER \
    metabrainz/listenbrainz-spark:latest \
    py.test
docker stop listenbrainz-spark-test-$USER
docker rm listenbrainz-spark-test-$USER
