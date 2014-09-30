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
package com.tango.BucketSyncer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class MirrorConstants {

    public static final long KB = 1024;
    public static final long MB = KB * 1024;
    public static final long GB = MB * 1024;
    public static final long TB = GB * 1024;
    public static final long PB = TB * 1024;
    public static final long EB = PB * 1024;

    public static final String CLIENT = "Client";
    public static final String STORAGE_CLIENT = "StorageClients";
    public static final String KEY_LISTER = "KeyLister";
    public static final String KEY_LISTERS = "KeyListers";
    public static final String KEY_COPY_JOB = "KeyCopyJob";
    public static final String MULTIPART_KEY_COPY_JOB = "MultipartKeyCopyJob";
    public static final String KEY_JOBS = "KeyJobs";
    public static final String KEY_DELETE_JOB = "KeyDeleteJob";

    public static final String GCS_CREDENTIAL_STORAGE_FILE = ".store/BucketSyncer";

    public static final String S3_PROTOCOL_PREFIX = "s3://";
    public static final String SLASH = "/";

    public static final String REPORT = "Report.txt";
    public static final Charset UTF8 = StandardCharsets.UTF_8;





}
