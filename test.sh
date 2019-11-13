#!/bin/bash

docker run \
    -v `pwd`:/rec
    --name listenbrainz-spark-test
    metabrainz/listenbrainz-spark:latest
    py.test
