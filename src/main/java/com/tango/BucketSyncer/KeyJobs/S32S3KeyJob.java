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

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Throwables;
import com.tango.BucketSyncer.MirrorContext;
import com.tango.BucketSyncer.MirrorOptions;
import com.tango.BucketSyncer.ObjectSummaries.ObjectSummary;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;

@Slf4j
public abstract class S32S3KeyJob extends KeyJob {

    protected final AmazonS3Client client;


    public S32S3KeyJob(Object sourceClient,
                       Object destClient,
                       MirrorContext context,
                       ObjectSummary summary,
                       Object notifyLock) {
        super(summary, notifyLock, context);
        this.client = (AmazonS3Client) sourceClient;
    }

    protected ObjectMetadata getObjectMetadata(String bucket, String key, MirrorOptions options) throws Exception {
        Exception ex = null;
        ObjectMetadata objectMetadat = null;
        for (int tries = 0; tries < options.getMaxRetries(); tries++) {
            try {
                context.getStats().getCount.incrementAndGet();
                objectMetadat = client.getObjectMetadata(bucket, key);
                break;

            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                    log.debug("Failed to fetch metadata of object: from S3, {}", key, e);
                    Throwables.propagate(e);
                }

            } catch (Exception e) {
                ex = e;
                if (tries >= options.getMaxRetries()) {
                    if (options.isVerbose()) {
                        log.error("getObjectMetadata( {} ) failed (try # {} ), giving up", key, tries);
                    }
                    break;
                } else if (options.isVerbose()) {
                    log.warn("getObjectMetadata( {} ) failed (try # {}), retrying...", key, tries);
                }
            }
        }
        if (objectMetadat != null) {
            return objectMetadat;
        } else {
            throw ex;
        }

    }

    protected AccessControlList getAccessControlList(MirrorOptions options, String key) throws Exception {
        Exception ex = null;
        AccessControlList acl = null;
        for (int tries = 0; tries < options.getMaxRetries(); tries++) {
            try {
                context.getStats().getCount.incrementAndGet();
                acl = client.getObjectAcl(options.getSourceBucket(), key);
                break;

            } catch (Exception e) {
                ex = e;
                if (tries >= options.getMaxRetries()) {
                    if (options.isVerbose()) {
                        log.error("getObjectAcl( {} ) failed (try # {} ), giving up", key, tries);
                    }
                    break;
                } else if (options.isVerbose()) {
                    log.warn("getObjectAcl( {} ) failed (try # {} ), retrying...", key, tries);
                }
            }
        }
        if (acl != null) {
            return acl;
        } else {
            throw ex;
        }
    }

}
