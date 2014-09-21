/**
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
import com.tango.BucketSyncer.MirrorContext;
import com.tango.BucketSyncer.MirrorOptions;
import com.tango.BucketSyncer.ObjectSummaries.ObjectSummary;
import lombok.extern.slf4j.Slf4j;

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
        for (int tries = 0; tries < options.getMaxRetries(); tries++) {
            try {
                context.getStats().getCount.incrementAndGet();
                return client.getObjectMetadata(bucket, key);

            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == 404) throw e;

            } catch (Exception e) {
                ex = e;
                if (options.isVerbose()) {
                    if (tries >= options.getMaxRetries()) {
                        log.error("getObjectMetadata( {} ) failed (try # {} ), giving up", key, tries);
                        break;
                    } else {
                        log.warn("getObjectMetadata( {} ) failed (try # {}), retrying...", key, tries);
                    }
                }
            }
        }
        throw ex;
    }

    protected AccessControlList getAccessControlList(MirrorOptions options, String key) throws Exception {
        Exception ex = null;
        for (int tries = 0; tries < options.getMaxRetries(); tries++) {
            try {
                context.getStats().getCount.incrementAndGet();
                return client.getObjectAcl(options.getSourceBucket(), key);

            } catch (Exception e) {
                ex = e;
                if (options.isVerbose()) {
                    if (tries >= options.getMaxRetries()) {
                        log.error("getObjectAcl( {} ) failed (try # {} ), giving up", key, tries);
                        break;
                    } else {
                        log.warn("getObjectAcl( {} ) failed (try # {} ), retrying...", key, tries);
                    }
                }
            }
        }
        throw ex;
    }

}
