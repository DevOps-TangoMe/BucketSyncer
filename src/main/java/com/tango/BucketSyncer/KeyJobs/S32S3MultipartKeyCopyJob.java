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
package com.tango.BucketSyncer.KeyJobs;

import com.amazonaws.services.s3.model.*;
import com.tango.BucketSyncer.MirrorContext;
import lombok.extern.slf4j.Slf4j;
import com.tango.BucketSyncer.MirrorOptions;
import com.tango.BucketSyncer.ObjectSummaries.ObjectSummary;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class S32S3MultipartKeyCopyJob extends S32S3KeyCopyJob {

    public S32S3MultipartKeyCopyJob(Object sourceClient, Object destClient, MirrorContext context, ObjectSummary summary, Object notifyLock) {
        super(sourceClient, destClient, context, summary, notifyLock);
    }

    @Override
    boolean keyCopied(ObjectMetadata sourceMetadata, AccessControlList objectAcl) {
        long objectSize = summary.getSize();
        MirrorOptions options = context.getOptions();
        String sourceBucketName = options.getSourceBucket();
        int maxPartRetries = options.getMaxRetries();
        String targetBucketName = options.getDestinationBucket();
        List<CopyPartResult> copyResponses = new ArrayList<CopyPartResult>();
        if (options.isVerbose()) {
            log.info("Initiating multipart upload request for {}", summary.getKey());
        }
        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(targetBucketName, keydest)
                .withObjectMetadata(sourceMetadata);

        if (options.isCrossAccountCopy()) {
            initiateRequest.withCannedACL(CannedAccessControlList.BucketOwnerFullControl);
        } else {
            initiateRequest.withAccessControlList(objectAcl);
        }

        InitiateMultipartUploadResult initResult = client.initiateMultipartUpload(initiateRequest);

        long partSize = options.getUploadPartSize();
        long bytePosition = 0;

        for (int i = 1; bytePosition < objectSize; i++) {
            long lastByte = bytePosition + partSize - 1 >= objectSize ? objectSize - 1 : bytePosition + partSize - 1;
            String infoMessage = String.format("Copying: %s to %s", bytePosition, lastByte);
            if (options.isVerbose()) {
                log.info(infoMessage);
            }
            CopyPartRequest copyRequest = new CopyPartRequest()
                    .withDestinationBucketName(targetBucketName)
                    .withDestinationKey(keydest)
                    .withSourceBucketName(sourceBucketName)
                    .withSourceKey(summary.getKey())
                    .withUploadId(initResult.getUploadId())
                    .withFirstByte(bytePosition)
                    .withLastByte(lastByte)
                    .withPartNumber(i);

            for (int tries = 1; tries <= maxPartRetries; tries++) {
                try {
                    if (options.isVerbose()) {
                        log.info("try : {}", tries);
                    }
                    context.getStats().copyCount.incrementAndGet();
                    CopyPartResult copyPartResult = client.copyPart(copyRequest);
                    copyResponses.add(copyPartResult);
                    if (options.isVerbose()) {
                        log.info("completed {} ", infoMessage);
                    }
                    break;
                } catch (Exception e) {
                    if (tries == maxPartRetries) {
                        client.abortMultipartUpload(new AbortMultipartUploadRequest(
                                targetBucketName, keydest, initResult.getUploadId()));
                        log.error("Exception while doing multipart copy: {}", e);
                        return false;
                    }
                }
            }
            bytePosition += partSize;
        }
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(targetBucketName, keydest,
                initResult.getUploadId(), getETags(copyResponses));
        client.completeMultipartUpload(completeRequest);
        if (options.isVerbose()) {
            log.info("completed multipart request for : {}", summary.getKey());
        }
        context.getStats().bytesCopied.addAndGet(objectSize);
        return true;
    }

    private List<PartETag> getETags(List<CopyPartResult> copyResponses) {
        List<PartETag> eTags = new ArrayList<PartETag>();
        for (CopyPartResult response : copyResponses) {
            eTags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return eTags;
    }

    @Override
    boolean objectChanged(ObjectMetadata metadata) {
        return summary.getSize() != metadata.getContentLength();
    }
}
