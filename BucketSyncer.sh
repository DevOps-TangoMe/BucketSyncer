#!/bin/bash

THISDIR=$(dirname $0)
cd ${THISDIR}
THISDIR=$(pwd)

VERSION=1.0.0
JARFILE=target/BucketSyncer-${VERSION}-SNAPSHOT.jar
VERSION_ARG="-DBucketSyncer.version=${VERSION}"

DEBUG=$1
if [ "${DEBUG}" = "--debug" ] ; then
  # Run in debug mode
  shift   # remove --debug from options
  java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005 ${VERSION_ARG} -jar ${JARFILE} "$@"

else
  # Run in regular mode
  java ${VERSION_ARG} -jar ${JARFILE} "$@"
fi

exit $?

