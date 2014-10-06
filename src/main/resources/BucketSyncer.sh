#!/bin/bash
#  Copyright 2013 Jonathan Cobb
#  Copyright 2014 TangoMe Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
THISDIR=$(dirname $0)
THISDIR=$(pwd)

#1.0.0-SNAPSHOT
FULLVERSION=${project.version}

VERSION=${FULLVERSION%"-SNAPSHOT"}

JARFILE=target/BucketSyncer-$FULLVERSION.jar
VERSION_ARG="-DBucketSyncer.version=$VERSION"

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

