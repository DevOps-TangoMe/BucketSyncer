/**
 *  Copyright 2013 Jonathan Cobb
 *  Copyright 2014 TangoMe Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.tango.BucketSyncer.KeyJobs;

import com.amazonaws.services.s3.model.*;
import com.tango.BucketSyncer.MirrorContext;
import com.tango.BucketSyncer.MirrorOptions;
import com.tango.BucketSyncer.MirrorStats;
import com.tango.BucketSyncer.ObjectSummaries.ObjectSummary;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;

@Slf4j
public class S32S3KeyDeleteJob extends S32S3KeyJob {

    private String keysrc;

    public S32S3KeyDeleteJob(Object sourceClient, Object destClient, MirrorContext context, ObjectSummary summary, Object notifyLock) {
        super(sourceClient, destClient, context, summary, notifyLock);

        final MirrorOptions options = context.getOptions();
        keysrc = summary.getKey(); // NOTE: summary.getKey is the key in the destination bucket
        if (options.hasPrefix()) {
            keysrc = keysrc.substring(options.getDestPrefixLength());
            keysrc = options.getPrefix() + keysrc;
        }
    }

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final MirrorStats stats = context.getStats();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();
        final String key = summary.getKey();
        try {
            if (!shouldDelete()) {
                return;
            }

            final DeleteObjectRequest request = new DeleteObjectRequest(options.getDestinationBucket(), key);

            if (options.isDryRun()) {
                log.info("Would have deleted {} from destination because {} does not exist in source", key, keysrc);
            } else {
                boolean deletedOK = false;
                for (int tries = 0; tries < maxRetries; tries++) {
                    if (verbose) {
                        log.info("deleting (try #{}): {}", tries, key);
                    }
                    try {
                        stats.deleteCount.incrementAndGet();
                        client.deleteObject(request);
                        deletedOK = true;
                        if (verbose) {
                            log.info("successfully deleted (on try # {}): {}", tries, key);
                        }
                        break;

                    } catch (AmazonS3Exception s3e) {
                        log.error("s3 exception deleting (try #{}) {}: {}", new Object[]{tries, key, s3e});

                    } catch (Exception e) {
                        log.error("unexpected exception deleting (try #{}) {}: {}", new Object[]{tries, key, e});
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        log.error("interrupted while waiting to retry key: {} ", key);
                        break;
                    }
                }
                if (deletedOK) {
                    stats.objectsDeleted.incrementAndGet();
                } else {
                    stats.deleteErrors.incrementAndGet();
                    //add fail-deleted key to errorKeyList
                    context.getStats().errorKeyList.add(key);
                }
            }

        } catch (Exception e) {
            log.error("error deleting key: {}: {}", key, e);

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (verbose) {
                log.info("done with {}", key);
            }
        }
    }

    private boolean shouldDelete() {
        boolean shouldDelete = false;
        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();

        try {
            ObjectMetadata metadata = getObjectMetadata(options.getSourceBucket(), keysrc, options);
            return shouldDelete; // object exists in source bucket, don't delete it from destination bucket

        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                if (verbose) {
                    log.info("Key not found in source bucket (will delete from destination): {}", keysrc);
                }
                shouldDelete = true;
                return shouldDelete;
            } else {
                log.warn("Error getting metadata for {} {} (not deleting): {}", new Object[]{options.getSourceBucket(), keysrc, e});
                return shouldDelete;
            }
        } catch (Exception e) {
            log.warn("Error getting metadata for {} {} (not deleting): {}", new Object[]{options.getSourceBucket(), keysrc, e});
            return shouldDelete;
        }
    }

}
