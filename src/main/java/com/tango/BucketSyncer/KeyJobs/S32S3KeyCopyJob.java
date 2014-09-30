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
import com.tango.BucketSyncer.*;
import com.tango.BucketSyncer.ObjectSummaries.ObjectSummary;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;

import java.util.Date;

/**
 * Handles a single key. Determines if it should be copied, and if so, performs the copy operation.
 */
@Slf4j
public class S32S3KeyCopyJob extends S32S3KeyJob {

    protected String keydest;

    public S32S3KeyCopyJob(Object sourceClient,
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

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final String key = summary.getKey();
        try {
            if (!shouldTransfer()) {
                return;
            }
            final ObjectMetadata sourceMetadata = getObjectMetadata(options.getSourceBucket(), key, options);
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
            log.error("Error copying key: {}: {}", key, e);

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (options.isVerbose()) {
                log.info("Done with {}", key);
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
        for (int tries = 0; tries < maxRetries; tries++) {
            if (verbose) {
                log.info("copying (try # {}): {} to: {}", new Object[]{tries, key, keydest});
            }
            final CopyObjectRequest request = new CopyObjectRequest(options.getSourceBucket(), key, options.getDestinationBucket(), keydest);
            request.setNewObjectMetadata(sourceMetadata);
            if (options.isCrossAccountCopy()) {
                request.setCannedAccessControlList(CannedAccessControlList.BucketOwnerFullControl);
            } else {
                request.setAccessControlList(objectAcl);
            }
            try {
                stats.copyCount.incrementAndGet();
                client.copyObject(request);
                stats.bytesCopied.addAndGet(sourceMetadata.getContentLength());
                if (verbose) {
                    log.info("successfully copied (on try #{}): {} to: {}", new Object[]{tries, key, keydest});
                }
                copied = true;
                break;
            } catch (AmazonS3Exception s3e) {
                //if return with 404 error, problem with bucket name
                if(s3e.getStatusCode() == HttpStatus.SC_NOT_FOUND){
                    log.error("Failed to access S3 bucket. Check bucket name: ", s3e);
                    System.exit(1);
                }
                log.error("s3 exception copying (try #{}) {} to: {}: {}", new Object[]{tries, key, keydest, s3e});
            } catch (Exception e) {
                log.error("unexpected exception copying (try #{}) {} to: {}: {}", new Object[]{tries, key, keydest, e});
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                log.error("interrupted while waiting to retry key: {}: {}", key, e);
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
                        log.info("key {} (lastmod = {}) is older than {} (cutoff = {}), not copying", new Object[]{key, lastModified, options.getCtime(), options.getMaxAgeDate()});
                    }
                    return false;
                }
            }
        }
        final ObjectMetadata metadata;
        try {
            metadata = getObjectMetadata(options.getDestinationBucket(), keydest, options);
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                if (verbose) {
                    log.debug("Key not found in destination bucket (will copy): {}", keydest);
                }
                return true;
            } else {
                log.warn("Error getting metadata for {} {} (not copying): {}", new Object[]{options.getDestinationBucket(), keydest, e});
                return false;
            }
        } catch (Exception e) {
            log.warn("Error getting metadata for {} {} (not copying): {}", new Object[]{options.getDestinationBucket(), keydest, e});
            return false;
        }

        if (summary.getSize() > MirrorOptions.MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE) {
            return metadata.getContentLength() != summary.getSize();
        }
        final boolean objectChanged = objectChanged(metadata);
        if (verbose && !objectChanged) {
            log.info("Destination file is same as source, not copying: {}", key);
        }

        return objectChanged;
    }

    boolean objectChanged(ObjectMetadata metadata) {
        final KeyFingerprint sourceFingerprint = new KeyFingerprint(summary.getSize(), summary.getETag());
        final KeyFingerprint destFingerprint = new KeyFingerprint(metadata.getContentLength(), metadata.getETag());
        return !sourceFingerprint.equals(destFingerprint);
    }
}
