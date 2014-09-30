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
package com.tango.BucketSyncer;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;

import static com.tango.BucketSyncer.MirrorOptions.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)

public class S3_MockTest {

    @Mock
    private AmazonS3Client s3;

    @Mock
    ObjectListing listing;

    @Mock
    AccessControlList objectAcl;

    CopyObjectResult copyObjectResult = new CopyObjectResult();


    String SOURCE = "source_bucket";
    String DESTINATION = "dest_bucket";

    private MirrorMain main = null;

    @Test
    public void testSimpleCopy() throws Exception {
        S3ObjectSummary testObjectSummary = new S3ObjectSummary();
        final String key = "test_object";
        final String[] args = {OPT_VERBOSE, OPT_SOURCE_BUCKET, SOURCE, OPT_DESTINATION_BUCKET, DESTINATION};
        testObjectSummary.setETag("etag");
        testObjectSummary.setBucketName("source_bucket");
        testObjectSummary.setKey("test_object");
        testObjectSummary.setSize(Long.valueOf(10));
        List<S3ObjectSummary> objectSummaries = new ArrayList<S3ObjectSummary>();
        objectSummaries.add(testObjectSummary);
        ArrayList<String> keys = new ArrayList<String>();
        keys.add(key);
        testSimpleCopyInternal(keys, args, objectSummaries);

    }

    @Test
    public void testSimpleCopyWithCtime() throws Exception {
        Date today = new Date();
        Calendar cal = new GregorianCalendar();
        cal.setTime(today);
        cal.add(Calendar.DAY_OF_MONTH, -30);
        Date past = cal.getTime();
        String copyKey = "copy";
        String notCopyKey = "not_copy";
        S3ObjectSummary copyObjectSummary = new S3ObjectSummary();
        copyObjectSummary.setETag("etag");
        copyObjectSummary.setBucketName("source_bucket");
        copyObjectSummary.setKey(copyKey);
        copyObjectSummary.setSize(Long.valueOf(10));
        copyObjectSummary.setLastModified(today);
        S3ObjectSummary notCopyObjectSummary = new S3ObjectSummary();
        notCopyObjectSummary.setETag("etag");
        notCopyObjectSummary.setBucketName("source_bucket");
        notCopyObjectSummary.setKey(notCopyKey);
        notCopyObjectSummary.setSize(Long.valueOf(10));
        notCopyObjectSummary.setLastModified(past);
        final String[] args = {OPT_VERBOSE, OPT_CTIME, "10d", OPT_SOURCE_BUCKET, SOURCE, OPT_DESTINATION_BUCKET, DESTINATION};

        List<S3ObjectSummary> objectSummaries = new ArrayList<S3ObjectSummary>();
        objectSummaries.add(copyObjectSummary);
        objectSummaries.add(notCopyObjectSummary);
        ArrayList<String> keys = new ArrayList<String>();
        keys.add(copyKey);
        keys.add(notCopyKey);
        testSimpleCopyInternal(keys, args, objectSummaries);

    }

    @Test
    public void testDeleteObject() throws Exception {
        //create one object for source and one for dest
        String copyObjectKey = "copy_object_from_source";
        String deleteObjectKey = "delete_object_from_dest";
        S3ObjectSummary copyObjectSummary = new S3ObjectSummary();
        copyObjectSummary.setETag("etag");
        copyObjectSummary.setBucketName("source_bucket");
        copyObjectSummary.setKey(copyObjectKey);
        copyObjectSummary.setSize(Long.valueOf(10));
        S3ObjectSummary deleteObjectSummary = new S3ObjectSummary();
        deleteObjectSummary.setETag("etag");
        deleteObjectSummary.setBucketName("source_bucket");
        deleteObjectSummary.setKey(deleteObjectKey);
        deleteObjectSummary.setSize(Long.valueOf(10));

        //set up
        main = new MirrorMain(new String[]{OPT_VERBOSE,
                OPT_DELETE_REMOVED, OPT_SOURCE_BUCKET, SOURCE, OPT_DESTINATION_BUCKET, DESTINATION});

        main.parseArguments();
        main.setSourceClient(s3);
        main.setDestClient(s3);
        MirrorOptions options = main.getOptions();
        MirrorContext context = new MirrorContext(options);
        main.setContext(context);
        MirrorMaster master = new MirrorMaster(s3, s3, context);
        AmazonS3Exception e = new AmazonS3Exception("Key not found");
        e.setStatusCode(404);
        main.setMaster(master);
        List<S3ObjectSummary> copyObjectSummaries = new ArrayList<S3ObjectSummary>();
        copyObjectSummaries.add(copyObjectSummary);
        List<S3ObjectSummary> deleteObjectSummaries = new ArrayList<S3ObjectSummary>();
        deleteObjectSummaries.add(deleteObjectSummary);

        //mock response from S3
        when(s3.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(listing);
        when(listing.getObjectSummaries()).thenReturn(copyObjectSummaries)
                .thenReturn(deleteObjectSummaries);

        //have to return a 404 error, so new file will be copied to dest_bucket
        when(s3.getObjectMetadata(DESTINATION, copyObjectKey)).thenThrow(e);
        when(s3.getObjectMetadata(SOURCE, deleteObjectKey)).thenThrow(e);

        //return ACL
        when(s3.getObjectAcl(SOURCE, copyObjectKey)).thenReturn(objectAcl);

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(Long.valueOf(10));

        when(s3.getObjectMetadata(SOURCE, copyObjectKey)).thenReturn(objectMetadata);
        when(s3.copyObject(Mockito.any(CopyObjectRequest.class))).thenReturn(copyObjectResult);


        main.init();
        main.run();

        //number of copied files
        assertEquals(1, main.getContext().getStats().objectsCopied.get());

        //size of total copied files
        assertEquals(10, main.getContext().getStats().bytesCopied.get());

        //number of deleted files
        assertEquals(1, main.getContext().getStats().objectsDeleted.get());

    }


    private void testSimpleCopyInternal(ArrayList<String> keys, String[] args, List<S3ObjectSummary> objectSummaries) throws Exception {

        main = new MirrorMain(args);
        main.parseArguments();
        main.setSourceClient(s3);
        main.setDestClient(s3);
        MirrorOptions options = main.getOptions();
        MirrorContext context = new MirrorContext(options);
        main.setContext(context);
        MirrorMaster master = new MirrorMaster(s3, s3, context);
        AmazonS3Exception e = new AmazonS3Exception("Key not found");
        e.setStatusCode(404);

        main.setMaster(master);

        when(s3.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(listing);
        when(listing.getObjectSummaries()).thenReturn(objectSummaries);

        //have to return a 404 error, so new file will be copied to dest_bucket
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(Long.valueOf(10));

        for (String key : keys) {
            when(s3.getObjectMetadata(DESTINATION, key)).thenThrow(e);
            //return ACL
            when(s3.getObjectAcl(SOURCE, key)).thenReturn(objectAcl);

            when(s3.getObjectMetadata(SOURCE, key)).thenReturn(objectMetadata);
        }

        when(s3.copyObject(Mockito.any(CopyObjectRequest.class))).thenReturn(copyObjectResult);

        main.init();

        main.run();

        //number of copied files
        assertEquals(1, main.getContext().getStats().objectsCopied.get());

        //size of total copied files
        assertEquals(10, main.getContext().getStats().bytesCopied.get());

    }


}
