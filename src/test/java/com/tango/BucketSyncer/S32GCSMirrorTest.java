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
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.tango.BucketSyncer.MirrorOptions.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Slf4j
public class S32GCSMirrorTest {

    public static final String SOURCE_ENV_VAR = "S3S3_TEST_SOURCE";
    public static final String DEST_ENV_VAR = "S3GCS_TEST_DEST";
    public static final String GCS_APPLICAION_NAME = "GCS_APP_NAME";

    public static final String SOURCE = System.getenv(SOURCE_ENV_VAR);
    public static final String DESTINATION = System.getenv(DEST_ENV_VAR);
    public static final String GCS_APP_NAME = System.getenv(GCS_APPLICAION_NAME);

    private List<StorageAsset> stuffToCleanup = new ArrayList<StorageAsset>();

    // Every individual test *must* initialize the "main" instance variable, otherwise NPE gets thrown here.
    private MirrorMain main = null;

    private S32GCSTestFile createS3TestFile(String key, S32GCSTestFile.Copy copy, S32GCSTestFile.Clean clean) throws Exception {
        return S32GCSTestFile.create(key, (AmazonS3Client) main.getSourceClient(), stuffToCleanup, copy, clean);
    }

    private S32GCSTestFile createGCSTestFile(String key, S32GCSTestFile.Copy copy, S32GCSTestFile.Clean clean) throws Exception {
        return S32GCSTestFile.create(key, (Storage) main.getDestClient(), stuffToCleanup, copy, clean);
    }


    public static String random(int size) {
        return RandomStringUtils.randomAlphanumeric(size) + "_" + System.currentTimeMillis();
    }

    private boolean checkEnvs() {
        if (SOURCE == null || DESTINATION == null) {
            log.warn("No " + SOURCE_ENV_VAR + " and or no " + DEST_ENV_VAR + " found in enviroment, skipping test");
            return false;
        }
        return true;
    }

    @After
    public void cleanupS3Assets() {
        // Every individual test *must* initialize the "main" instance variable, otherwise NPE gets thrown here.

        if (checkEnvs()) {
            Storage gcsClient = (Storage) main.getDestClient();
            AmazonS3Client s3Client = (AmazonS3Client) main.getSourceClient();
            for (StorageAsset asset : stuffToCleanup) {
                try {
                    log.info("cleanup GCS: deleting " + asset);
                    gcsClient.objects().delete(DESTINATION, asset.key).execute();
                } catch (Exception e) {
                    log.error("Error cleaning up object from GCS: " + asset + ": " + e.getMessage());
                }
                try {
                    log.info("cleanup S3: deleting " + asset);
                    s3Client.deleteObject(SOURCE, asset.key);
                } catch (Exception e) {
                    log.error("Error cleaning up object from S3: " + asset + ": " + e.getMessage());
                }
            }
            main = null;
        }
    }

    @Test
    public void testSimpleCopy() throws Exception {
        if (!checkEnvs()) return;
        final String key = "testSimpleCopy_" + random(10);
        final String[] args = {OPT_VERBOSE, OPT_PREFIX, key, SOURCE, DESTINATION, OPT_DEST_STORE, "GCS", OPT_GCS_APPLICAION_NAME, GCS_APP_NAME};

        testSimpleCopyInternal(key, args);
    }

    @Test
    public void testSkipCopyingNotChangedFile() throws Exception {
        if (!checkEnvs()) return;
        String prefix = "testSkipCopyingNotChangedFile_";
        final String key1 = "testSkipCopyingNotChangedFile_" + random(10);
        final String key2 = "testSkipCopyingNotChangedFile_" + random(10);
        final String[] args = {OPT_VERBOSE, OPT_PREFIX, prefix, SOURCE, DESTINATION, OPT_DEST_STORE, "GCS", OPT_GCS_APPLICAION_NAME, GCS_APP_NAME};

        testSimpleCopyInternal(key1, args);
        testSimpleCopyInternal(key2, args);

    }

    @Test
    public void testSimpleCopyWithInlinePrefix() throws Exception {
        if (!checkEnvs()) return;
        final String key = "testSimpleCopyWithInlinePrefix_" + random(10);
        final String[] args = {OPT_VERBOSE, SOURCE + "/" + key, DESTINATION, OPT_DEST_STORE, "GCS", OPT_GCS_APPLICAION_NAME, GCS_APP_NAME};

        testSimpleCopyInternal(key, args);
    }

    private void testSimpleCopyInternal(String key, String[] args) throws Exception {

        main = new MirrorMain(args);
        main.init();

        final S32GCSTestFile s32GCSTestFile = createS3TestFile(key, S32GCSTestFile.Copy.SOURCE, S32GCSTestFile.Clean.SOURCE_AND_DEST);

        main.run();

        assertEquals(1, main.getContext().getStats().objectsCopied.get());
        assertEquals(s32GCSTestFile.data.length(), main.getContext().getStats().bytesCopied.get());


        final Storage.Objects.Get getObject = ((Storage) main.getDestClient()).objects().get(DESTINATION, key);
        StorageObject gcsObject = getObject.execute();
        assertEquals(s32GCSTestFile.data.length(), gcsObject.getSize().intValue());
    }

    @Test
    public void testSimpleCopyWithDestPrefix() throws Exception {
        if (!checkEnvs()) return;
        final String key = "testSimpleCopyWithDestPrefix_" + random(10);
        final String destKey = "dest_testSimpleCopyWithDestPrefix_" + random(10);
        final String[] args = {OPT_PREFIX, key, OPT_DEST_PREFIX, destKey, SOURCE, DESTINATION, OPT_DEST_STORE, "GCS", OPT_GCS_APPLICAION_NAME, GCS_APP_NAME};
        testSimpleCopyWithDestPrefixInternal(key, destKey, args);
    }

    @Test
    public void testSimpleCopyWithCtime() throws Exception {
        if (!checkEnvs()) return;
        final String prefix = "testSimpleCopyWithCtime";
        final String key1 = prefix + random(10);
        final String key2 = prefix + random(10);
        final String[] args1 = {OPT_VERBOSE, OPT_PREFIX, prefix, SOURCE, DESTINATION, OPT_DEST_STORE, "GCS", OPT_GCS_APPLICAION_NAME, GCS_APP_NAME};
        final String[] args2 = {OPT_VERBOSE, OPT_PREFIX, prefix, SOURCE, DESTINATION, OPT_DEST_STORE, "GCS", OPT_GCS_APPLICAION_NAME, GCS_APP_NAME, OPT_CTIME, "1s"};
        testSimpleCopyInternal(key1, args1);
        Thread.sleep(2000);
        testSimpleCopyInternal(key2, args2);
    }


    @Test
    public void testSimpleCopyWithInlineDestPrefix() throws Exception {
        if (!checkEnvs()) return;
        final String key = "testSimpleCopyWithInlineDestPrefix_" + random(10);
        final String destKey = "dest_testSimpleCopyWithInlineDestPrefix_" + random(10);
        final String[] args = {SOURCE + "/" + key, DESTINATION + "/" + destKey, OPT_DEST_STORE, "GCS", OPT_GCS_APPLICAION_NAME, GCS_APP_NAME};
        testSimpleCopyWithDestPrefixInternal(key, destKey, args);
    }

    private void testSimpleCopyWithDestPrefixInternal(String key, String destKey, String[] args) throws Exception {
        main = new MirrorMain(args);
        main.init();

        final S32GCSTestFile s32GCSTestFile = createS3TestFile(key, S32GCSTestFile.Copy.SOURCE, S32GCSTestFile.Clean.SOURCE);
        stuffToCleanup.add(new StorageAsset(DESTINATION, destKey));

        main.run();

        assertEquals(1, main.getContext().getStats().objectsCopied.get());
        assertEquals(s32GCSTestFile.data.length(), main.getContext().getStats().bytesCopied.get());

        final Storage.Objects.Get getObject = ((Storage) main.getDestClient()).objects().get(DESTINATION, key);
        StorageObject gcsObject = getObject.execute();
        assertEquals(s32GCSTestFile.data.length(), gcsObject.getSize().intValue());

    }

    @Test
    public void testDeleteRemoved() throws Exception {
        if (!checkEnvs()) return;

        final String key = "testDeleteRemoved_" + random(10);

        main = new MirrorMain(new String[]{OPT_VERBOSE, OPT_PREFIX, key,
                OPT_DELETE_REMOVED, SOURCE, DESTINATION, OPT_CTIME, "2d", OPT_DEST_STORE, "GCS", OPT_GCS_APPLICAION_NAME, GCS_APP_NAME});

        main.init();

        // Write some files to dest
        final int numDestFiles = 3;
        final String[] destKeys = new String[numDestFiles];
        final S32GCSTestFile[] destFiles = new S32GCSTestFile[numDestFiles];
        for (int i = 0; i < numDestFiles; i++) {
            destKeys[i] = key + "-dest" + i;
            destFiles[i] = createGCSTestFile(destKeys[i], S32GCSTestFile.Copy.DEST, S32GCSTestFile.Clean.DEST);
        }

        // Write 1 file to source
        final String srcKey = key + "-src";
        final S32GCSTestFile srcFile = createS3TestFile(srcKey, S32GCSTestFile.Copy.SOURCE, S32GCSTestFile.Clean.SOURCE_AND_DEST);

        // Initiate copy
        main.run();

        // Expect only 1 copy and numDestFiles deletes
        // Expect none of the original dest files to be there anymore
        for (int i = 0; i < numDestFiles; i++) {
            try {
                Storage.Objects.Get getObject = ((Storage) main.getDestClient()).objects().get(DESTINATION, destKeys[i]);
                getObject.execute();
                fail("testDeleteRemoved: expected " + destKeys[i] + " to be removed from destination bucket " + DESTINATION);
            } catch (Exception e) {

            }
        }

        // Expect source file to now be present in both source and destination buckets
        ObjectMetadata metadata;
        metadata = ((AmazonS3Client) main.getSourceClient()).getObjectMetadata(SOURCE, srcKey);
        assertEquals(srcFile.data.length(), metadata.getContentLength());

        //check gcsObject size
        Storage.Objects.Get getObject = ((Storage) main.getDestClient()).objects().get(DESTINATION, srcKey);
        StorageObject gcsObject = getObject.execute();
        assertEquals(srcFile.data.length(), gcsObject.getSize().intValue());
    }

}
