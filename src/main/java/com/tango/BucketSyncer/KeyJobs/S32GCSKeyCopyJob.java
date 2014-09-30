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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.*;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.tango.BucketSyncer.*;
import com.tango.BucketSyncer.ObjectSummaries.ObjectSummary;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import org.apache.http.HttpStatus;

@Slf4j
public class S32GCSKeyCopyJob extends S32GCSKeyJob {


    protected String keydest;

    public S32GCSKeyCopyJob(Object sourceClient,
                            Object destClient,
                            MirrorContext context,
                            ObjectSummary summary,
                            Object notifyLock) {
        super(sourceClient, destClient, context, summary, notifyLock);

        keydest = summary.getKey();
        final MirrorOptions options = context.getOptions();
        if (options.hasDestPrefix()) {
            keydest = keydest.substring(options.getPrefixLength());
            keydest = options.getDestPrefix() + keydest;
        }
    }

    public void run() {
        final MirrorOptions options = context.getOptions();
        final String key = summary.getKey();
        try {
            if (!shouldTransfer()) {
                return;
            }
            final ObjectMetadata sourceMetadata = getS3ObjectMetadata(options.getSourceBucket(), key, options);
            final AccessControlList objectAcl = getAccessControlList(options, key);

            if (options.isDryRun()) {
                log.info("Would have copied {} to destination: {}", key, keydest);
            } else {
                if (keyCopied(sourceMetadata, objectAcl)) {
                    context.getStats().objectsCopied.incrementAndGet();
                } else {
                    context.getStats().copyErrors.incrementAndGet();
                    //add fail-copied key to errorKeyList
                    context.getStats().errorKeyList.add(key);
                }
            }
        } catch (Exception e) {
            log.error("error copying key: {}: {}", key, e);

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (options.isVerbose()) {
                log.info("done with {} ", key);
            }
        }
    }

    boolean keyCopied(ObjectMetadata sourceMetadata, AccessControlList objectAcl) {
        boolean copied = false;
        String key = summary.getKey();
        MirrorOptions options = context.getOptions();
        boolean verbose = options.isVerbose();
        int maxRetries = options.getMaxRetries();
        MirrorStats stats = context.getStats();

        InputStreamContent mediaContent = null;

        for (int tries = 0; tries < maxRetries; tries++) {

            if (verbose) {
                log.info("copying (try # {} ): {} to: {}", new Object[]{tries, key, keydest});
            }

            //get object from S3
            //deal with exception that the object has been deleted when trying to fetch it from S3
            S3Object s3object = null;

            try {
                s3object = s3Client.getObject(new GetObjectRequest(
                        options.getSourceBucket(), key));
            } catch (AmazonServiceException e) {
                log.error("Failed to fetch object from S3. Object {} may have been deleted: {}", key, e);
            } catch (Exception e) {
                log.error("Failed to fetch object from S3. Object {} may have been deleted: {}", key, e);
            }

            if (s3object != null) {

                InputStream inputStream = s3object.getObjectContent();

                String type = s3object.getObjectMetadata().getContentType();
                mediaContent = new InputStreamContent(type, inputStream);

                String etag = s3object.getObjectMetadata().getETag();
                StorageObject objectMetadata = new StorageObject()
                        .setMetadata(ImmutableMap.of("Etag", etag));


                Storage.Objects.Insert insertObject = null;
                try {
                    insertObject = gcsClient.objects().insert(options.getDestinationBucket(), objectMetadata, mediaContent);
                } catch (IOException e) {
                    log.error("Failed to create insertObject of GCS ", e);
                }

                insertObject.setName(key);


                insertObject.getMediaHttpUploader()
                        .setProgressListener(new CustomUploadProgressListener()).setDisableGZipContent(true);

                // For small files, you may wish to call setDirectUploadEnabled(true), to
                // reduce the number of HTTP requests made to the server.

                if (mediaContent.getLength() > 0 && mediaContent.getLength() <= 2 * 1000 * 1000 /* 2MB */) {
                    insertObject.getMediaHttpUploader().setDirectUploadEnabled(true);
                }

                try {
                    stats.copyCount.incrementAndGet();
                    insertObject.execute();
                    stats.bytesCopied.addAndGet(sourceMetadata.getContentLength());
                    if (verbose)
                        log.info("Successfully copied (on try # {} ): {} to: {} in GCS", new Object[]{tries, key, keydest});
                    copied = true;
                    break;
                } catch (GoogleJsonResponseException e) {
                    if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                        log.error("Failed to access GCS bucket. Check bucket name: ", e);
                        System.exit(1);
                    }
                }catch (IOException e) {
                    log.error("GCS exception copying (try # {} ) {} to: {} : {}", new Object[]{tries, key, keydest, e});
                }
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                log.error("interrupted while waiting to retry key: {}, {}", key, e);
                return copied;
            }
        }
        return copied;
    }

    private boolean shouldTransfer() {
        final MirrorOptions options = context.getOptions();
        final String key = summary.getKey();
        final boolean verbose = options.isVerbose();

        if (options.hasCtime()) {
            final Date lastModified = summary.getLastModified();
            if (lastModified == null) {
                if (verbose) {
                    log.info("No Last-Modified header for key: {}", key);
                }
            } else {
                if (lastModified.getTime() < options.getMaxAge()) {
                    if (verbose) {
                        log.info("key {} (lastmod = {} ) is older than {} (cutoff= {}), not copying", new Object[]{key, lastModified, options.getCtime(), options.getMaxAgeDate()});
                    }
                    return false;
                }
            }
        }

        final StorageObject metadata;
        try {
            metadata = getGCSObjectMetadata(options.getDestinationBucket(), keydest, options);
        } catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() ==  HttpStatus.SC_BAD_REQUEST) {
                log.error("Failed to talk to GCS. It may caused by invalid GCS credentials. Please provide valid credentials: ", e);
                System.exit(1);
            }
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                if (verbose) {
                    log.info("Key not found in GCS bucket (will copy to GCS): {}", keydest);
                }
                return true;
            } else {
                log.error("Error getting metadata for destination bucket: {}, object: {} (not copying to GCS): {}", new Object[]{options.getDestinationBucket(), keydest, e});
                return false;
            }
        } catch (Exception e) {
            log.error("Error getting metadata for destination bucket: {}, object: {} (not copying to GCS): {}", new Object[]{options.getDestinationBucket(), keydest, e});
            return false;
        }

        if (summary.getSize() > MirrorOptions.MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE) {
            log.warn("File is too large to be uploaded");
            return false;
        }
        final boolean objectChanged = objectChanged(metadata);
        if (verbose && !objectChanged) {
            log.info("Destination file is same as source, not copying to GCS: {}", key);
        }
        return objectChanged;

    }


    boolean objectChanged(StorageObject metadata) {
        final KeyFingerprint sourceFingerprint = new KeyFingerprint(summary.getSize(), summary.getETag());

        final KeyFingerprint destFingerprint = new KeyFingerprint(metadata.getSize().longValue(), metadata.getMetadata().get("Etag"));
        return !sourceFingerprint.equals(destFingerprint);
    }

    /*copy from google api sample: https://code.google.com/p/google-api-java-client/source/browse/storage-cmdline-sample/src/main/java/com/google/api/services/samples/storage/cmdline/StorageSample.java?repo=samples&r=3c0563b18af378906144f9eec0979b0a1427352b
     */

    private static class CustomUploadProgressListener implements MediaHttpUploaderProgressListener {
        private final Stopwatch stopwatch = new Stopwatch();

        public CustomUploadProgressListener() {
        }

        @Override
        public void progressChanged(MediaHttpUploader uploader) {
            switch (uploader.getUploadState()) {
                case INITIATION_STARTED:
                    stopwatch.start();
                    log.info("Initiation has started!");
                    break;
                case INITIATION_COMPLETE:
                    log.info("Initiation is complete!");
                    break;
                case MEDIA_IN_PROGRESS:
                    log.info("Uploaded {} bytes of {}...", uploader.getNumBytesUploaded());
                    break;
                case MEDIA_COMPLETE:
                    stopwatch.stop();
                    log.info(String.format("Upload is complete! (%s)", stopwatch));
                    break;
                case NOT_STARTED:
                    break;
            }
        }
    }

}
