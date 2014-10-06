# Copyright 2014 TangoMe Inc.
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

import time
import os
import subprocess
import logging, logging.config
import argparse
from pytimeparse.timeparse import timeparse
from subprocess import CalledProcessError, check_output
from notifier.notifier import create as create_notifier
from xml.etree.ElementTree import ElementTree
from future import *
import ConfigParser

from apscheduler.schedulers.background import BackgroundScheduler

sourceClient = None
destClient = None
sourceBucket = None
destBucket = None
gcsAppName = None
verbose = False
deleteRemove = False
ctime = None
prefix = None
destPrefix = None
endpoint = None
maxConnection = None
dryRun = False
maxRetries = None
uploadPartSize = None
proxy = None
crossAccountCopy = False
maxThreads = None
interval = None
logger = None
reportInterval = 14400

FULL_VERSION = '${project.version}'
JARFILE = 'target/BucketSyncer-%s.jar' % FULL_VERSION
VERSION_NO = FULL_VERSION[0:FULL_VERSION.find('-SNAPSHOT')]
VERSION_ARG = '-DBucketSyncer.version=%s' % VERSION_NO
cfg = 'config/config.cfg'

ctxt = {
    'notifiers': {}
}

logFile = "/var/log/BucketSyncer/BucketSyncer.log"
reportFile = "/var/log/BucketSyncer/Report.txt"
las_pos = None


def publishToSNS(subject, message):
    logger.info('Sending report to SNS')
    try:
        for notifier in ctxt['notifiers']:
            notifier.publish_message(subject, message)
    except Exception as e:
        logger.warn('Failed to notify[{0}]:{1}'.format(notifier, e))
        raise RuntimeError('Failed to publish to AmazonSNS: {0}'.format(e))


def reportGenerator():
    global las_pos
    try:
        fo = open(reportFile, "rw+")
    except Exception as e:
        logger.warn('Failed to find report.txt: {0}'.format(e))
        raise RuntimeError('Failed to find report.txt: {0}'.format(e))
    if las_pos is None:
        las_pos = 0
    fo.seek(las_pos)
    lines = fo.read().splitlines()
    las_pos = fo.tell()
    fo.close()
    report = ''
    for line in lines:
        report += '\n' + line
    publishToSNS('BucketSyncer Report', report)


def logReportGenerator():
    try:
        with open(logFile, "rw+") as fo:
            fsize = fo.tell()  # Get Size
            fo.seek (max (fsize-1024, 0), 0) # Set pos @ last n chars
            lines = fo.readlines()
    except Exception as e:
        logger.warn('Failed to read from BucketSyncer.log: {0}'.format(e))
        raise RuntimeError('Failed to read from BucketSyncer.log: {0}'.format(e))

    lines = lines[-100:]
    report = ''
    for line in lines:
        report += '\n' + line
    publishToSNS('BucketSyncer Aborted', report)

def bucket_sync():
    command_list = ['java', VERSION_ARG, '-jar', JARFILE]

    command_list.append('-F')
    command_list.append(sourceBucket)
    command_list.append('-T')
    command_list.append(destBucket)

    if (sourceClient):
        command_list.append('-S')
        command_list.append(sourceClient)

    if (destClient):
        command_list.append('-D')
        command_list.append(destClient)

    if (gcsAppName):
        command_list.append('-A')
        command_list.append(gcsAppName)

    if (verbose):
        command_list.append('-v')

    if (deleteRemove):
        command_list.append('-X')

    if (ctime):
        command_list.append('-c')
        command_list.append(ctime)

    if (destPrefix):
        command_list.append('-d')
        command_list.append(destPrefix)

    if (prefix):
        command_list.append('-p')
        command_list.append(prefix)

    if (endpoint):
        command_list.append('-e')
        command_list.append(endpoint)

    if (maxConnection):
        command_list.append('-m')
        command_list.append(maxConnection)

    if (dryRun):
        command_list.append('-n')

    if (maxRetries):
        command_list.append('-r')

    if (uploadPartSize):
        command_list.append('-u')
        command_list.append(uploadPartSize)

    if (proxy):
        command_list.append('-z')
        command_list.append(proxy)

    if (crossAccountCopy):
        command_list.append('-C')

    if (maxThreads):
        command_list.append('-t')
        command_list.append(maxThreads)

    if (verbose):
        logger.info(command_list)

    # exit when BucketSyncer exit with exit code 1
    try:
        subprocess.check_output(command_list)
    except CalledProcessError as e:
        logger.info('BucketSyncer exited.')
        logReportGenerator()
        reportGenerator()
        os._exit(1)

def main():
    global sourceClient, destClient, sourceBucket, destBucket, gcsAppName, verbose, deleteRemove, ctime, prefix, destPrefix, endpoint, maxConnection, dryRun, maxRetries, uploadPartSize, proxy, crossAccountCopy, maxThreads, logger, reportInterval


    parser = argparse.ArgumentParser(description='Sync Buckets Between Cloud Storage')

    parser.add_argument('-F', '--source_bucket', required=True,
                        help='Name of source bucket')

    parser.add_argument('-T', '--dest_bucket', required=True,
                        help='Name of destination bucket')

    parser.add_argument('-A', '--gcs_app_name',
                        help='Name of GCS Application Name')

    parser.add_argument('-C', '--cross_account_copy', action='store_true', default=False,
                        help='Copy across AWS accounts. Default is False')

    parser.add_argument('-D', '--dest_store_type',
                        help='Destination cloud storage type. Default is S3')

    parser.add_argument('-c', '--ctime',
                        help='Only copy objects whose Last-Modified date is younger than this many days. For other time units, use these suffixes: y (years), M (months), d (days), w (weeks), h (hours), m (minutes), s (seconds)')

    parser.add_argument('-d', '--dest_prefix',
                        help='Destination prefix')

    parser.add_argument('-e', '--endpoint',
                        help='AWS endpoint')

    parser.add_argument('-i', '--interval', required=True,
                        help='Intervals (second) to run BucketSyncer. (Format example: 2s, 23m, 2h32m, 4:13, 5hr2m3s, 1.2 minutes)')

    parser.add_argument('-I', '--report_interval',
                        help='Intervals (second) to report BucketSyncer status. Default is 4 hours.(Format example: 2s, 23m, 2h32m, 4:13, 5hr2m3s, 1.2 minutes)')

    parser.add_argument('-m', '--max_connections',
                        help='Maximum number of connections to S3. Default is 100.')

    parser.add_argument('-n', '--dry_run', action='store_true', default=False,
                        help='Show what would be done')

    parser.add_argument('-p', '--prefix',
                        help='Only copy objects whose keys start with this prefix')

    parser.add_argument('-r', '--max_retries',
                        help='Maximum number of retries for requests. Default is 5')

    parser.add_argument('-S', '--source_store_type',
                        help='Source cloud storage type. Default is S3. Currently, only S3 is supported')

    parser.add_argument('-t', '--max_threads',
                        help='Maximum number of threads. Default is 100.')

    parser.add_argument('-u', '--upload_part_size',
                        help='The upload size (in bytes) of each part uploaded as part of a multipart request for files that are greater than the max allowed file size of 5368709120 bytes (5GB). Defaults to 4294967296 bytes (4GB).')

    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='Verbose output')

    parser.add_argument('-X', '--delete_removed', action='store_true', default=False,
                        help='Delete objects from the destination bucket if they do not exist in the source bucket')

    parser.add_argument('-z', '--proxy',
                        help='host:port of proxy server to use. Defaults to proxy_host and proxy_port defined in config/s3cfg.properties, or no proxy if these values are not found in s3cfg.properties')

    options = parser.parse_args()

    sourceBucket = options.source_bucket
    destBucket = options.dest_bucket
    gcsAppName = options.gcs_app_name
    crossAccountCopy = options.cross_account_copy
    destClient = options.dest_store_type
    sourceClient = options.source_store_type
    deleteRemove = options.delete_removed
    ctime = options.ctime
    destPrefix = options.dest_prefix
    endpoint = options.endpoint
    maxConnection = options.max_connections
    dryRun = options.dry_run
    prefix = options.prefix
    maxRetries = options.max_retries
    maxThreads = options.max_threads
    uploadPartSize = options.upload_part_size
    verbose = options.verbose
    proxy = options.proxy
    interval = timeparse(options.interval)
    if options.report_interval is not None:
        reportInterval = timeparse(options.report_interval)

    # get config file, setup notifier
    config = ConfigParser.ConfigParser()
    try:
        config.read(cfg)
    except Exception as e:
        logger.warn('Failed to read the config file')
        raise RuntimeError('Failed to read the config file:{0}'.format(e))
    ctxt['notifiers'] = create_notifier(config, logger)

    # setup log
    logging.config.fileConfig(cfg)
    logger = logging.getLogger('MAIN.syncer')

    # setup scheduler
    scheduler = BackgroundScheduler({'apscheduler.timezone': 'UTC',})
    scheduler.add_job(bucket_sync, 'interval', seconds=interval)
    scheduler.add_job(reportGenerator, 'interval', seconds=reportInterval)

    scheduler.start()

    logger.info('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()  # Not strictly necessary if daemonic mode is enabled but should be done if possible



if __name__ == '__main__':
    main()