#!/bin/bash

docker build -t metabrainz/listenbrainz-spark -f Dockerfile.dev . && \
    docker push metabrainz/listenbrainz-spark
