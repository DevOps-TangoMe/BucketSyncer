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
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.tango.BucketSyncer.MirrorOptions.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
/*
This test suit requires S3 credentials. Please provide S3 credentials in s3cfg.properties and set up source and destination buckets before enabling the following tests.
 */
@Slf4j
public class S32S3MirrorTest {

    public static final String SOURCE_ENV_VAR = "S3S3_TEST_SOURCE";
    public static final String DEST_ENV_VAR = "S3S3_TEST_DEST";

    public static final String SOURCE = System.getenv(SOURCE_ENV_VAR);
    public static final String DESTINATION = System.getenv(DEST_ENV_VAR);

    private List<StorageAsset> stuffToCleanup = new ArrayList<StorageAsset>();

    // Every individual test *must* initialize the "main" instance variable, otherwise NPE gets thrown here.
    private MirrorMain main = null;

    private S32S3TestFile createTestFile(String key, S32S3TestFile.Copy copy, S32S3TestFile.Clean clean) throws Exception {
        return S32S3TestFile.create(key, (AmazonS3Client) main.getSourceClient(), stuffToCleanup, copy, clean);
    }

    public static String random(int size) {
        return RandomStringUtils.randomAlphanumeric(size) + "_" + System.currentTimeMillis();
    }

    private boolean checkEnvs() {
        if (SOURCE == null || DESTINATION == null) {
            log.warn("No " + SOURCE_ENV_VAR + " and/or no " + DEST_ENV_VAR + " found in enviroment, skipping test");
            return false;
        }
        return true;
    }

    @After
    public void cleanupS3Assets() {
        // Every individual test *must* initialize the "main" instance variable, otherwise NPE gets thrown here.
        if (checkEnvs()) {
            AmazonS3Client client = (AmazonS3Client) main.getSourceClient();
            for (StorageAsset asset : stuffToCleanup) {
                try {
                    log.info("cleanupS3Assets: deleting " + asset);
                    client.deleteObject(asset.bucket, asset.key);
                } catch (Exception e) {
                    log.error("Error cleaning up object: " + asset + ": " + e.getMessage());
                }
            }
            main = null;
        }
    }

    @Test
    public void testSimpleCopy() throws Exception {
        if (!checkEnvs()) return;
        final String key = "testSimpleCopy_" + random(10);
        final String[] args = {OPT_VERBOSE, OPT_PREFIX, key, OPT_SOURCE_BUCKET, SOURCE, OPT_DESTINATION_BUCKET, DESTINATION};

        testSimpleCopyInternal(key, args);
    }
    //@Ignore
    @Test
    public void testSimpleCopyWithInlinePrefix() throws Exception {
        if (!checkEnvs()) return;
        final String key = "testSimpleCopyWithInlinePrefix_" + random(10);
        final String[] args = {OPT_VERBOSE, OPT_SOURCE_BUCKET, SOURCE + "/" + key, OPT_DESTINATION_BUCKET, DESTINATION};

        testSimpleCopyInternal(key, args);
    }

    //@Ignore
    @Test
    public void testSimpleCopyWithCtime() throws Exception {
        if (!checkEnvs()) return;
        final String prefix = "testSimpleCopyWithCtime";
        final String key1 = prefix + random(10);
        final String key2 = prefix + random(10);
        final String[] args1 = {OPT_VERBOSE, OPT_PREFIX, prefix, OPT_SOURCE_BUCKET, SOURCE, OPT_DESTINATION_BUCKET, DESTINATION};
        final String[] args2 = {OPT_VERBOSE, OPT_PREFIX, prefix, OPT_SOURCE_BUCKET, SOURCE, OPT_DESTINATION_BUCKET, DESTINATION, OPT_CTIME, "1s"};
        testSimpleCopyInternal(key1, args1);
        Thread.sleep(2000);
        testSimpleCopyInternal(key2, args2);
    }

    @Test
    public void testMultipleBatch() throws Exception{
        if (!checkEnvs()) return;
        //put three objects into source while thread size is two, so need to call s3getNextBatch()
        final String prefix = "testMultipleBatch";
        final String key1 = prefix + random(10);
        final String key2 = prefix + random(10);
        final String key3 = prefix + random(10);
        final String[] args = {OPT_VERBOSE, OPT_PREFIX, prefix, OPT_SOURCE_BUCKET, SOURCE, OPT_DESTINATION_BUCKET, DESTINATION, OPT_MAX_THREADS, "2"};

        main = new MirrorMain(args);
        main.init();
        final S32S3TestFile s32S3TestFile1 = createTestFile(key1, S32S3TestFile.Copy.SOURCE, S32S3TestFile.Clean.SOURCE_AND_DEST);
        final S32S3TestFile s32S3TestFile2 = createTestFile(key2, S32S3TestFile.Copy.SOURCE, S32S3TestFile.Clean.SOURCE_AND_DEST);
        final S32S3TestFile s32S3TestFile3 = createTestFile(key3, S32S3TestFile.Copy.SOURCE, S32S3TestFile.Clean.SOURCE_AND_DEST);

        main.run();
        assertEquals(3, main.getContext().getStats().objectsCopied.get());
    }

    private void testSimpleCopyInternal(String key, String[] args) throws Exception {

        main = new MirrorMain(args);
        main.init();

        final S32S3TestFile s32S3TestFile = createTestFile(key, S32S3TestFile.Copy.SOURCE, S32S3TestFile.Clean.SOURCE_AND_DEST);

        main.run();

        assertEquals(1, main.getContext().getStats().objectsCopied.get());
        assertEquals(s32S3TestFile.data.length(), main.getContext().getStats().bytesCopied.get());

        final ObjectMetadata metadata = ((AmazonS3Client) main.getSourceClient()).getObjectMetadata(DESTINATION, key);
        assertEquals(s32S3TestFile.data.length(), metadata.getContentLength());
    }

    @Test
    public void testSimpleCopyWithDestPrefix() throws Exception {
        if (!checkEnvs()) return;
        final String key = "testSimpleCopyWithDestPrefix_" + random(10);
        final String destKey = "dest_testSimpleCopyWithDestPrefix_" + random(10);
        final String[] args = {OPT_PREFIX, key, OPT_DEST_PREFIX, destKey, OPT_SOURCE_BUCKET, SOURCE, OPT_DESTINATION_BUCKET, DESTINATION};
        testSimpleCopyWithDestPrefixInternal(key, destKey, args);
    }

    @Test
    public void testSimpleCopyWithInlineDestPrefix() throws Exception {
        if (!checkEnvs()) return;
        final String key = "testSimpleCopyWithInlineDestPrefix_" + random(10);
        final String destKey = "dest_testSimpleCopyWithInlineDestPrefix_" + random(10);
        final String[] args = {OPT_SOURCE_BUCKET, SOURCE + "/" + key, OPT_DESTINATION_BUCKET, DESTINATION + "/" + destKey};
        testSimpleCopyWithDestPrefixInternal(key, destKey, args);
    }

    @Test
    public void testSkipCopyingNotChangedFile() throws Exception {
        if (!checkEnvs()) return;
        String prefix = "testSkipCopyingNotChangedFile_";
        final String key1 = "testSkipCopyingNotChangedFile_" + random(10);
        final String key2 = "testSkipCopyingNotChangedFile_" + random(10);
        final String[] args = {OPT_VERBOSE, OPT_PREFIX, prefix, OPT_SOURCE_BUCKET, SOURCE, OPT_DESTINATION_BUCKET, DESTINATION};

        testSimpleCopyInternal(key1, args);
        testSimpleCopyInternal(key2, args);

    }

    private void testSimpleCopyWithDestPrefixInternal(String key, String destKey, String[] args) throws Exception {
        main = new MirrorMain(args);
        main.init();

        final S32S3TestFile s32S3TestFile = createTestFile(key, S32S3TestFile.Copy.SOURCE, S32S3TestFile.Clean.SOURCE);
        stuffToCleanup.add(new StorageAsset(DESTINATION, destKey));

        main.run();

        assertEquals(1, main.getContext().getStats().objectsCopied.get());
        assertEquals(s32S3TestFile.data.length(), main.getContext().getStats().bytesCopied.get());

        final ObjectMetadata metadata = ((AmazonS3Client) main.getSourceClient()).getObjectMetadata(DESTINATION, destKey);
        assertEquals(s32S3TestFile.data.length(), metadata.getContentLength());
    }

    @Test
    public void testDeleteRemoved() throws Exception {
        if (!checkEnvs()) return;

        final String key = "testDeleteRemoved_" + random(10);

        main = new MirrorMain(new String[]{OPT_VERBOSE, OPT_PREFIX, key,
                OPT_DELETE_REMOVED, OPT_SOURCE_BUCKET, SOURCE, OPT_DESTINATION_BUCKET, DESTINATION});
        main.init();

        // Write some files to dest
        final int numDestFiles = 3;
        final String[] destKeys = new String[numDestFiles];
        final S32S3TestFile[] destFiles = new S32S3TestFile[numDestFiles];
        for (int i = 0; i < numDestFiles; i++) {
            destKeys[i] = key + "-dest" + i;
            destFiles[i] = createTestFile(destKeys[i], S32S3TestFile.Copy.DEST, S32S3TestFile.Clean.DEST);
        }

        // Write 1 file to source
        final String srcKey = key + "-src";
        final S32S3TestFile srcFile = createTestFile(srcKey, S32S3TestFile.Copy.SOURCE, S32S3TestFile.Clean.SOURCE_AND_DEST);

        // Initiate copy
        main.run();

        // Expect only 1 copy and numDestFiles deletes
        assertEquals(1, main.getContext().getStats().objectsCopied.get());
        assertEquals(numDestFiles, main.getContext().getStats().objectsDeleted.get());

        // Expect none of the original dest files to be there anymore
        for (int i = 0; i < numDestFiles; i++) {
            try {
                ((AmazonS3Client) main.getSourceClient()).getObjectMetadata(DESTINATION, destKeys[i]);
                fail("testDeleteRemoved: expected " + destKeys[i] + " to be removed from destination bucket " + DESTINATION);
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() != 404) {
                    fail("testDeleteRemoved: unexpected exception (expected statusCode == 404): " + e);
                }
            }
        }

        // Expect source file to now be present in both source and destination buckets
        ObjectMetadata metadata;
        metadata = ((AmazonS3Client) main.getSourceClient()).getObjectMetadata(SOURCE, srcKey);
        assertEquals(srcFile.data.length(), metadata.getContentLength());

        metadata = ((AmazonS3Client) main.getSourceClient()).getObjectMetadata(DESTINATION, srcKey);
        assertEquals(srcFile.data.length(), metadata.getContentLength());
    }

}
