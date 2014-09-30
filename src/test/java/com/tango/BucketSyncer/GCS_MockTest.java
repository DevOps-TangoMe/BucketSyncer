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
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.*;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static com.tango.BucketSyncer.MirrorOptions.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;

@RunWith(PowerMockRunner.class)
@PrepareForTest({GoogleJsonResponseException.class, MediaHttpUploader.class, AbstractGoogleClientRequest.class, com.google.api.services.storage.model.Objects.class})
@PowerMockIgnore("javax.management.*")
public class GCS_MockTest {

    private AmazonS3Client s3 = PowerMockito.mock(AmazonS3Client.class);
    private Storage gcs = PowerMockito.mock(Storage.class);
    private ObjectListing listing = PowerMockito.mock(ObjectListing.class);
    private AccessControlList objectAcl = PowerMockito.mock(AccessControlList.class);

    String SOURCE = "s3_bucket";
    String DESTINATION = "gcs_bucket";

    private MirrorMain main = null;

    @Test
    public void testSimpleDelete() throws Exception {

        main = new MirrorMain(new String[]{OPT_VERBOSE,
                OPT_DELETE_REMOVED, OPT_SOURCE_BUCKET, SOURCE, OPT_DESTINATION_BUCKET, DESTINATION, OPT_DEST_STORE, "GCS", OPT_GCS_APPLICAION_NAME, "TangoMe"});
        main.parseArguments();
        main.setSourceClient(s3);
        main.setDestClient(gcs);
        MirrorOptions options = main.getOptions();
        MirrorContext context = new MirrorContext(options);
        main.setContext(context);
        MirrorMaster master = new MirrorMaster(s3, gcs, context);
        main.setMaster(master);

        //set up
        Storage.Objects objects = PowerMockito.mock(Storage.Objects.class);
        Storage.Objects.List listObjects = PowerMockito.mock(Storage.Objects.List.class);

        //create StorageObject and add it to return list of objects
        StorageObject object = new StorageObject();
        object.setBucket(DESTINATION);
        object.setEtag("etag");
        String key = "test_file";
        object.setName(key);
        object.setSize(BigInteger.valueOf(10));
        List<StorageObject> listOfObjects = new ArrayList<StorageObject>();
        listOfObjects.add(object);
        Objects mockobjects = PowerMockito.mock(Objects.class);
        List<S3ObjectSummary> objectSummaries = new ArrayList<S3ObjectSummary>();
        AmazonS3Exception e = new AmazonS3Exception("Key not found");
        e.setStatusCode(404);
        Storage.Objects.Delete result = PowerMockito.mock(Storage.Objects.Delete.class);


        PowerMockito.when(gcs.objects()).thenReturn(objects);
        PowerMockito.when(gcs.objects().list(DESTINATION)).thenReturn(listObjects);
        PowerMockito.when(gcs.objects().list(DESTINATION).setMaxResults(any(Long.class))).thenReturn(listObjects);
        PowerMockito.when(gcs.objects().list(DESTINATION).setMaxResults(any(Long.class)).setPrefix(any(String.class))).thenReturn(listObjects);
        PowerMockito.when(listObjects.execute()).thenReturn(mockobjects);
        PowerMockito.when(mockobjects.getItems()).thenReturn(listOfObjects);
        PowerMockito.when(s3.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(listing);
        PowerMockito.when(listing.getObjectSummaries()).thenReturn(objectSummaries);
        PowerMockito.when(s3.getObjectMetadata(SOURCE, key)).thenThrow(e);
        PowerMockito.when(gcs.objects().delete(DESTINATION, key)).thenReturn(result);

        main.init();
        main.run();

        //number of deleted files
        assertEquals(1, main.getContext().getStats().objectsDeleted.get());

    }

    @Test
    public void testSimpleCopy() throws Exception {

        S3ObjectSummary testObjectSummary = new S3ObjectSummary();
        final String key = "test_object";
        final String[] args = {OPT_VERBOSE, OPT_SOURCE_BUCKET, SOURCE, OPT_DESTINATION_BUCKET, DESTINATION, OPT_DEST_STORE, "GCS", OPT_GCS_APPLICAION_NAME, "TangoMe"};
        testObjectSummary.setETag("etag");
        testObjectSummary.setBucketName("source_bucket");
        testObjectSummary.setKey("test_object");
        testObjectSummary.setSize(Long.valueOf(10));
        List<S3ObjectSummary> objectSummaries = new ArrayList<S3ObjectSummary>();
        objectSummaries.add(testObjectSummary);
        testSimpleCopyInternal(key, args, objectSummaries);
    }

    private void testSimpleCopyInternal(String key, String[] args, List<S3ObjectSummary> objectSummaries) throws Exception {
        main = new MirrorMain(args);
        main.parseArguments();
        main.setSourceClient(s3);
        main.setDestClient(gcs);
        MirrorOptions options = main.getOptions();
        MirrorContext context = new MirrorContext(options);
        main.setContext(context);
        MirrorMaster master = new MirrorMaster(s3, gcs, context);

        GoogleJsonResponseException e = PowerMockito.mock(GoogleJsonResponseException.class);
        PowerMockito.when(e.getStatusCode()).thenReturn(404);


        main.setMaster(master);

        PowerMockito.when(s3.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(listing);
        PowerMockito.when(listing.getObjectSummaries()).thenReturn(objectSummaries);

        Storage.Objects objects = PowerMockito.mock(Storage.Objects.class);
        Storage.Objects.Get getObject = PowerMockito.mock(Storage.Objects.Get.class);
        PowerMockito.when(gcs.objects()).thenReturn(objects);
        PowerMockito.when(gcs.objects().get(DESTINATION, key)).thenReturn(getObject);
        PowerMockito.when(getObject.execute()).thenThrow(e);

        //mock object from S3
        S3Object s3Object = new S3Object();
        s3Object.setKey(key);
        String content = "S3Object_Content";
        InputStream inputStream = new ByteArrayInputStream(content.getBytes());
        s3Object.setObjectContent(inputStream);
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType("text");
        objectMetadata.setContentLength(10);
        objectMetadata.setHeader(Headers.ETAG, "etag");

        s3Object.setObjectMetadata(objectMetadata);
        s3Object.setBucketName(SOURCE);

        //return ACL
        PowerMockito.when(s3.getObjectAcl(SOURCE, key)).thenReturn(objectAcl);

        PowerMockito.when(s3.getObjectMetadata(SOURCE, key)).thenReturn(objectMetadata);

        PowerMockito.when(s3.getObject(any(GetObjectRequest.class))).thenReturn(s3Object);


        Storage.Objects.Insert insertObject = PowerMockito.mock(Storage.Objects.Insert.class);
        PowerMockito.when(gcs.objects().insert(any(String.class), any(StorageObject.class), any(InputStreamContent.class))).thenReturn(insertObject);

        MediaHttpUploader mediaHttpUploader = PowerMockito.mock(MediaHttpUploader.class);
        PowerMockito.when(insertObject.getMediaHttpUploader()).thenReturn(mediaHttpUploader);
        PowerMockito.when(mediaHttpUploader.setProgressListener(any(MediaHttpUploaderProgressListener.class))).thenReturn(mediaHttpUploader);
        PowerMockito.when(mediaHttpUploader.setDisableGZipContent(any(boolean.class))).thenReturn(mediaHttpUploader);

        main.init();
        main.run();

        //number of copied files
        assertEquals(1, main.getContext().getStats().objectsCopied.get());

        //size of total copied files
        assertEquals(10, main.getContext().getStats().bytesCopied.get());
    }


}
