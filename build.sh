#!/usr/bin/env bash


if test "$TRAVIS_PULL_REQUEST" = "false"
then
    if [ -z "$TRAVIS_TAG" ];
    then
      ./gradlew test
    else
        echo "Deploying to bintray"
        ./gradlew bintrayUpload
    fi
else
    ./gradlew test
fi