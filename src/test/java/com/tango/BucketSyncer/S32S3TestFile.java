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
import lombok.Cleanup;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.math.RandomUtils;

import java.io.*;
import java.util.List;

class S32S3TestFile {

    public static final int TEST_FILE_SIZE = 1024;

    enum Copy {SOURCE, DEST, SOURCE_AND_DEST}

    enum Clean {SOURCE, DEST, SOURCE_AND_DEST}

    public File file;
    public String data;

    public S32S3TestFile() throws Exception {
        file = File.createTempFile(getClass().getName(), ".tmp");
        data = S32S3MirrorTest.random(TEST_FILE_SIZE + (RandomUtils.nextInt() % 1024));
        @Cleanup FileOutputStream out = new FileOutputStream(file);
        IOUtils.copy(new ByteArrayInputStream(data.getBytes()), out);
        file.deleteOnExit();
    }

    public static S32S3TestFile create(String key, AmazonS3Client client, List<StorageAsset> stuffToCleanup, Copy copy, Clean clean) throws Exception {
        S32S3TestFile s32S3TestFile = new S32S3TestFile();
        switch (clean) {
            case SOURCE:
                stuffToCleanup.add(new StorageAsset(S32S3MirrorTest.SOURCE, key));
                break;
            case DEST:
                stuffToCleanup.add(new StorageAsset(S32S3MirrorTest.DESTINATION, key));
                break;
            case SOURCE_AND_DEST:
                stuffToCleanup.add(new StorageAsset(S32S3MirrorTest.SOURCE, key));
                stuffToCleanup.add(new StorageAsset(S32S3MirrorTest.DESTINATION, key));
                break;
        }
        switch (copy) {
            case SOURCE:
                client.putObject(S32S3MirrorTest.SOURCE, key, s32S3TestFile.file);
                break;
            case DEST:
                client.putObject(S32S3MirrorTest.DESTINATION, key, s32S3TestFile.file);
                break;
            case SOURCE_AND_DEST:
                client.putObject(S32S3MirrorTest.SOURCE, key, s32S3TestFile.file);
                client.putObject(S32S3MirrorTest.DESTINATION, key, s32S3TestFile.file);
                break;
        }
        return s32S3TestFile;
    }
}
