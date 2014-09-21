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
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import lombok.Cleanup;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.math.RandomUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class S32GCSTestFile {
    public static final int TEST_FILE_SIZE = 1024;

    enum Copy {SOURCE, DEST, SOURCE_AND_DEST}

    enum Clean {SOURCE, DEST, SOURCE_AND_DEST}

    public File file;
    public String data;

    public S32GCSTestFile() throws Exception {
        file = File.createTempFile(getClass().getName(), ".tmp");
        data = S32GCSMirrorTest.random(TEST_FILE_SIZE + (RandomUtils.nextInt() % 1024));
        @Cleanup FileOutputStream out = new FileOutputStream(file);
        IOUtils.copy(new ByteArrayInputStream(data.getBytes()), out);
        file.deleteOnExit();
    }

    public static S32GCSTestFile create(String key, AmazonS3Client client, List<StorageAsset> stuffToCleanup, Copy copy, Clean clean) throws Exception {
        S32GCSTestFile s32GCSTestFile = new S32GCSTestFile();
        switch (clean) {
            case SOURCE:
                stuffToCleanup.add(new StorageAsset(S32GCSMirrorTest.SOURCE, key));
                break;
            case DEST:
                stuffToCleanup.add(new StorageAsset(S32GCSMirrorTest.DESTINATION, key));
                break;
            case SOURCE_AND_DEST:
                stuffToCleanup.add(new StorageAsset(S32GCSMirrorTest.SOURCE, key));
                stuffToCleanup.add(new StorageAsset(S32GCSMirrorTest.DESTINATION, key));
                break;
        }
        switch (copy) {
            case SOURCE:
                client.putObject(S32GCSMirrorTest.SOURCE, key, s32GCSTestFile.file);
                break;
            case DEST:
                client.putObject(S32GCSMirrorTest.DESTINATION, key, s32GCSTestFile.file);
                break;
            case SOURCE_AND_DEST:
                client.putObject(S32GCSMirrorTest.SOURCE, key, s32GCSTestFile.file);
                client.putObject(S32GCSMirrorTest.DESTINATION, key, s32GCSTestFile.file);
                break;
        }
        return s32GCSTestFile;
    }

    public static S32GCSTestFile create(String key, Storage client, List<StorageAsset> stuffToCleanup, Copy copy, Clean clean) throws Exception {
        S32GCSTestFile s32GCSTestFile = new S32GCSTestFile();
        Storage.Objects.Insert insertObject = null;
        StorageObject objectMetadata = null;
        InputStream inputStream = new FileInputStream(s32GCSTestFile.file);
        Path source = Paths.get(s32GCSTestFile.file.getPath());
        String type = Files.probeContentType(source);
        InputStreamContent mediaContent = new InputStreamContent(type, inputStream);
        switch (clean) {
            case SOURCE:
                stuffToCleanup.add(new StorageAsset(S32GCSMirrorTest.SOURCE, key));
                break;
            case DEST:
                stuffToCleanup.add(new StorageAsset(S32GCSMirrorTest.DESTINATION, key));
                break;
            case SOURCE_AND_DEST:
                stuffToCleanup.add(new StorageAsset(S32GCSMirrorTest.SOURCE, key));
                stuffToCleanup.add(new StorageAsset(S32GCSMirrorTest.DESTINATION, key));
                break;
        }
        switch (copy) {
            case SOURCE:
                insertObject = client.objects().insert(S32GCSMirrorTest.SOURCE, objectMetadata, mediaContent);
                insertObject.setName(key);
                insertObject.execute();
                break;
            case DEST:
                insertObject = client.objects().insert(S32GCSMirrorTest.DESTINATION, objectMetadata, mediaContent);
                insertObject.setName(key);
                insertObject.execute();
                break;
            case SOURCE_AND_DEST:
                insertObject = client.objects().insert(S32GCSMirrorTest.SOURCE, objectMetadata, mediaContent);
                insertObject.setName(key);
                insertObject.execute();
                insertObject = client.objects().insert(S32GCSMirrorTest.DESTINATION, objectMetadata, mediaContent);
                insertObject.setName(key);
                insertObject.execute();
                break;
        }
        return s32GCSTestFile;
    }


}
