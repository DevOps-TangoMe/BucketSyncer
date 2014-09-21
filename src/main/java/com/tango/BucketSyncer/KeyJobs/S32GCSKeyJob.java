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
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Throwables;
import com.tango.BucketSyncer.MirrorContext;
import com.tango.BucketSyncer.MirrorOptions;
import com.tango.BucketSyncer.ObjectSummaries.ObjectSummary;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;


@Slf4j
public abstract class S32GCSKeyJob extends KeyJob {

    protected final AmazonS3Client s3Client;
    protected final Storage gcsClient;


    public S32GCSKeyJob(Object sourceClient,
                        Object destClient,
                        MirrorContext context,
                        ObjectSummary summary,
                        Object notifyLock) {
        super(summary, notifyLock, context);
        this.s3Client = (AmazonS3Client) sourceClient;
        this.gcsClient = (Storage) destClient;
    }

    //get object meta data from S3
    protected ObjectMetadata getS3ObjectMetadata(String bucket, String key, MirrorOptions options) throws Exception {
        Exception ex = null;
        for (int tries = 0; tries < options.getMaxRetries(); tries++) {
            try {
                context.getStats().getCount.incrementAndGet();
                return s3Client.getObjectMetadata(bucket, key);

            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == 404) throw e;

            } catch (Exception e) {
                ex = e;
                if (options.isVerbose()) {
                    if (tries >= options.getMaxRetries()) {
                        log.error("getObjectMetadata({}) failed (try #{}), giving up", key, tries);
                        break;
                    } else {
                        log.warn("getObjectMetadata({}) failed (try #{}), retrying...", key, tries);
                    }
                }
            }
        }
        throw ex;
    }


    //get object meta data from GCS
    protected StorageObject getGCSObjectMetadata(String bucket, String key, MirrorOptions options) throws Exception {

        Exception ex = null;
        Storage.Objects.Get getObject = null;
        StorageObject gcsObject = null;
        for (int tries = 0; tries < options.getMaxRetries(); tries++) {
            try {
                getObject = gcsClient.objects().get(bucket, key);
            } catch (IOException e) {
                e.printStackTrace();
                Throwables.propagate(e);
            }
            try {
                gcsObject = getObject.execute();
                return gcsObject;
            } catch (IOException e) {
                ex = e;
                if (options.isVerbose()) {
                    if (tries >= options.getMaxRetries()) {
                        log.error("getObjectMetadata({}) failed (try #{}), giving up", key, tries);
                        break;
                    } else {
                        log.warn("getObjectMetadata({}) failed (try #{}), retrying...", key, tries);
                    }
                }
            }
        }
        //need to modify
        throw ex;
    }

    //get ACL from S3 objects
    protected AccessControlList getAccessControlList(MirrorOptions options, String key) throws Exception {
        Exception ex = null;
        for (int tries = 0; tries < options.getMaxRetries(); tries++) {
            try {
                context.getStats().getCount.incrementAndGet();
                return s3Client.getObjectAcl(options.getSourceBucket(), key);

            } catch (Exception e) {
                ex = e;
                if (options.isVerbose()) {
                    if (tries >= options.getMaxRetries()) {
                        log.error("getObjectAcl({}) failed (try #{}), giving up", key, tries);
                        break;
                    } else {
                        log.warn("getObjectAcl({}) failed (try #{}), retrying...", key, tries);
                    }
                }
            }
        }
        throw ex;
    }
}
