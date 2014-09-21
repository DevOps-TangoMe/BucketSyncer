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
package com.tango.BucketSyncer.KeyListers;

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.tango.BucketSyncer.*;
import com.tango.BucketSyncer.ObjectSummaries.GCS_ObjectSummary;
import com.tango.BucketSyncer.ObjectSummaries.ObjectSummary;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class GCSKeyLister extends KeyLister {
    private Storage gcsClient;
    private final List<ObjectSummary> summaries;
    private Objects objects;


    public GCSKeyLister(Object client,
                        String bucket,
                        String prefix,
                        MirrorContext context,
                        Integer maxQueueCapacity) {
        super(bucket, prefix, context, maxQueueCapacity);
        this.gcsClient = (Storage) client;
        final MirrorOptions options = context.getOptions();
        int fetchSize = options.getMaxThreads();
        this.summaries = new ArrayList<ObjectSummary>(10 * fetchSize);

        objects = gcsGetFirstBatch(bucket, prefix, Long.valueOf(fetchSize));
        synchronized (summaries) {

            for (StorageObject object : objects.getItems()) {
                // Add to object summaries
                ObjectSummary os = new GCS_ObjectSummary(object);
                summaries.add(os);
            }
            context.getStats().objectsRead.addAndGet(objects.size());
            if (options.isVerbose()) {
                log.info("added initial set of {} keys", objects.size());
            }
        }
    }

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        int counter = 0;
        int fetchSize = options.getMaxThreads();
        log.info("starting...");
        try {
            while (true) {
                while (getSize() < maxQueueCapacity) {
                    if (objects.getNextPageToken() != null) {
                        objects = gcsGetNextBatch(bucket, prefix, Long.valueOf(fetchSize));
                        if (++counter % 100 == 0) {
                            context.getStats().logStats();
                        }
                        synchronized (summaries) {
                            for (StorageObject object : objects.getItems()) {
                                // Add to object summaries
                                ObjectSummary os = new GCS_ObjectSummary(object);
                                summaries.add(os);
                            }
                            context.getStats().objectsRead.addAndGet(objects.size());
                            if (verbose)
                                log.info("queued next set of {} keys (total now= {})", objects.size(), getSize());
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
            log.error("Error in run loop, KeyLister thread now exiting: ", e);
        } finally {
            if (verbose) {
                log.info("KeyLister run loop finished");
            }
            done.set(true);
        }
    }


    private Objects gcsGetFirstBatch(String bucket, String prefix, Long fetchSize) {

        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();

        Exception lastException = null;
        for (int tries = 0; tries < maxRetries; tries++) {
            Storage.Objects.List listObjects = null;
            try {
                listObjects = gcsClient.objects().list(bucket).setMaxResults(fetchSize).setPrefix(prefix);
            } catch (IOException e) {
                lastException = e;
                log.warn("gcsGetFirstBatch: error listing (try # {} ): ", tries, e);
                if (Sleep.sleep(50)) {
                    log.info("gcsGetFirstBatch: interrupted while waiting for next try");
                    break;
                }
            }
            Objects objects;
            try {
                context.getStats().getCount.incrementAndGet();
                objects = listObjects.execute();
                if (verbose) {
                    log.info("successfully got first batch of objects from GCS (on try # {})", tries);
                }
                return objects;
            } catch (IOException e) {
                lastException = e;
                log.warn("gcsGetFirstBatch: error listing (try # {}): ", tries, e);
                if (Sleep.sleep(50)) {
                    log.info("gcsGetFirstBatch: interrupted while waiting for next try");
                    break;
                }
            }

        }
        throw new IllegalStateException("gcsGetFirstBatch: error listing: " + lastException, lastException);
    }

    private Objects gcsGetNextBatch(String bucket, String prefix, Long fetchSize) {

        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();

        Storage.Objects.List listObjects = null;

        for (int tries = 0; tries < maxRetries; tries++) {
            context.getStats().getCount.incrementAndGet();
            try {
                listObjects = gcsClient.objects().list(bucket).setMaxResults(fetchSize).setPrefix(prefix);
            } catch (IOException e) {
                log.error("GCS exception listing objects (try # {} ): {}", tries, e);
            }
            listObjects.setPageToken(objects.getNextPageToken());
            try {
                context.getStats().getCount.incrementAndGet();
                Objects next = listObjects.execute();
                if (verbose) {
                    log.info("successfully got next batch of objects (on try # {} )", tries);
                }
                return next;
            } catch (IOException e) {
                log.error("GCS exception listing objects (try # {} ): {}", tries, e);
            }
            if (Sleep.sleep(50)) {
                log.info("gcsGetNextBatch: interrupted while waiting for next try");
                break;
            }
        }
        throw new IllegalStateException(String.format("Too many errors trying to list objects (maxRetries= %d)", maxRetries));
    }


    private int getSize() {
        synchronized (summaries) {
            return summaries.size();
        }
    }

    public List<ObjectSummary> getNextBatch() {
        List<ObjectSummary> copy;
        synchronized (summaries) {
            copy = new ArrayList<ObjectSummary>(summaries);
            summaries.clear();
        }
        return copy;
    }

}
