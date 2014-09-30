/**
 *  Copyright 2013 Jonathan Cobb
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
package com.tango.BucketSyncer.KeyListers;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.tango.BucketSyncer.*;
import com.tango.BucketSyncer.ObjectSummaries.ObjectSummary;
import com.tango.BucketSyncer.ObjectSummaries.S3_ObjectSummary;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class S3KeyLister extends KeyLister {
    private AmazonS3Client s3Client;
    private final List<S3ObjectSummary> summaries;
    private ObjectListing listing;


    public S3KeyLister(Object client,
                       String bucket,
                       String prefix,
                       MirrorContext context,
                       Integer maxQueueCapacity) {
        super(bucket, prefix, context, maxQueueCapacity);
        this.s3Client = (AmazonS3Client) client;

        final MirrorOptions options = context.getOptions();
        int fetchSize = options.getMaxThreads();
        this.summaries = new ArrayList<S3ObjectSummary>(10 * fetchSize);

        final ListObjectsRequest request = new ListObjectsRequest(bucket, prefix, null, null, fetchSize);
        listing = s3getFirstBatch(s3Client, request);
        synchronized (summaries) {
            final List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
            summaries.addAll(objectSummaries);
            context.getStats().objectsRead.addAndGet(objectSummaries.size());
            if (options.isVerbose()) {
                log.info("added initial set of {} keys", objectSummaries.size());
            }
        }
    }

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        int counter = 0;
        log.info("list s3 object starting...");
        try {
            while (true) {
                while (getSize() < maxQueueCapacity) {
                    if (listing.isTruncated()) {
                        listing = s3GetNextBatch();
                        if (++counter % 100 == 0) {
                            context.getStats().logStats();
                        }
                        synchronized (summaries) {
                            final List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
                            summaries.addAll(objectSummaries);
                            context.getStats().objectsRead.addAndGet(objectSummaries.size());
                            if (verbose) {
                                log.info("queued next set of {} keys (total now= {})", objectSummaries.size(), getSize());
                            }
                        }

                    } else {
                        log.info("No more keys found in source bucket, exiting");
                        return;
                    }
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    log.error("interrupted!");
                    return;
                }
            }
        } catch (Exception e) {
            log.error("Error in run loop, KEY_LISTER thread now exiting: {}", e);

        } finally {
            if (verbose) {
                log.info("KEY_LISTER run loop finished");
            }
            done.set(true);
        }
    }

    private ObjectListing s3getFirstBatch(AmazonS3Client client, ListObjectsRequest request) {

        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();
        ObjectListing listing = null;

        Exception lastException = null;
        for (int tries = 0; tries < maxRetries; tries++) {
            try {
                context.getStats().getCount.incrementAndGet();
                listing = client.listObjects(request);
                if (verbose) {
                    log.info("successfully got first batch of objects (on try # {})", tries);
                }
                break;
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == HttpStatus.SC_FORBIDDEN) {
                    if (verbose) {
                        log.error("Please provide valid AWS credentials: {}", e);
                    }
                }
                System.exit(1);
            } catch (Exception e) {
                lastException = e;
                log.warn("s3getFirstBatch: error listing (try # {}): {}", tries, e);
                if (Sleep.sleep(50)) {
                    log.info("s3getFirstBatch: interrupted while waiting for next try");
                    break;
                }
            }
        }
        if(listing!= null){
            return listing;
        }else {
            throw new

                    IllegalStateException("s3getFirstBatch: error listing: " + lastException, lastException);
        }
    }

    private ObjectListing s3GetNextBatch() {
        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();
        ObjectListing next = null;
        for (int tries = 0; tries < maxRetries; tries++) {
            try {
                context.getStats().getCount.incrementAndGet();
                next = s3Client.listNextBatchOfObjects(listing);
                if (verbose) {
                    log.info("successfully got next batch of objects (on try # {})", tries);
                }
                break;

            } catch (AmazonS3Exception s3e) {
                log.error("s3 exception listing objects (try # {}): {}", tries, s3e);

            } catch (Exception e) {
                log.error("unexpected exception listing objects (try # {}): {}", tries, e);
            }
            if (Sleep.sleep(50)) {
                log.info("s3GetNextBatch: interrupted while waiting for next try");
                break;
            }
        }
        if(next != null){
            return next;
        }else {
            throw new IllegalStateException(String.format("Too many errors trying to list objects (maxRetries= %d)", maxRetries));
        }

    }

    private int getSize() {
        synchronized (summaries) {
            return summaries.size();
        }
    }

    private List<S3ObjectSummary> getNextBatchOfS3ObjectSummary() {
        List<S3ObjectSummary> copy;
        synchronized (summaries) {
            copy = new ArrayList<S3ObjectSummary>(summaries);
            summaries.clear();
        }
        return copy;
    }


    public List<ObjectSummary> getNextBatch() {
        List<S3ObjectSummary> s3ObjectSummaries = getNextBatchOfS3ObjectSummary();

        List<ObjectSummary> objectSummaries = new ArrayList<ObjectSummary>(s3ObjectSummaries.size());

        for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries) {
            objectSummaries.add(new S3_ObjectSummary(s3ObjectSummary));
        }

        return objectSummaries;
    }

}
