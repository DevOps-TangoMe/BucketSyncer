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

import com.amazonaws.auth.AWSCredentials;
import lombok.Getter;
import lombok.Setter;
import org.joda.time.DateTime;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import java.util.Date;

public class MirrorOptions implements AWSCredentials {

    public static final String AWS_ACCESS_KEY = "AWS_ACCESS_KEY_ID";
    public static final String AWS_SECRET_KEY = "AWS_SECRET_ACCESS_KEY";
    @Getter
    @Setter
    private String aWSAccessKeyId = System.getenv().get(AWS_ACCESS_KEY);
    @Getter
    @Setter
    private String aWSSecretKey = System.getenv().get(AWS_SECRET_KEY);

    public boolean hasAwsKeys() {
        return aWSAccessKeyId != null && aWSSecretKey != null;
    }

    public static final String USAGE_DRY_RUN = "Do not actually do anything, but show what would be done";
    public static final String OPT_DRY_RUN = "-n";
    public static final String LONGOPT_DRY_RUN = "--dry-run";
    @Option(name = OPT_DRY_RUN, aliases = LONGOPT_DRY_RUN, usage = USAGE_DRY_RUN)
    @Getter
    @Setter
    private boolean dryRun = false;

    public static final String USAGE_VERBOSE = "Verbose output";
    public static final String OPT_VERBOSE = "-v";
    public static final String LONGOPT_VERBOSE = "--verbose";
    @Option(name = OPT_VERBOSE, aliases = LONGOPT_VERBOSE, usage = USAGE_VERBOSE)
    @Getter
    @Setter
    private boolean verbose = false;

    public static final String USAGE_PREFIX = "Only copy objects whose keys start with this prefix";
    public static final String OPT_PREFIX = "-p";
    public static final String LONGOPT_PREFIX = "--prefix";
    @Option(name = OPT_PREFIX, aliases = LONGOPT_PREFIX, usage = USAGE_PREFIX)
    @Getter
    @Setter
    private String prefix = null;

    public boolean hasPrefix() {
        return prefix != null && prefix.length() > 0;
    }

    public int getPrefixLength() {
        return prefix == null ? 0 : prefix.length();
    }

    public static final String USAGE_DEST_PREFIX = "Destination prefix (replacing the one specified in --prefix, if any)";
    public static final String OPT_DEST_PREFIX = "-d";
    public static final String LONGOPT_DEST_PREFIX = "--dest-prefix";
    @Option(name = OPT_DEST_PREFIX, aliases = LONGOPT_DEST_PREFIX, usage = USAGE_DEST_PREFIX)
    @Getter
    @Setter
    private String destPrefix = null;


    public boolean hasDestPrefix() {
        return destPrefix != null && destPrefix.length() > 0;
    }

    public int getDestPrefixLength() {
        return destPrefix == null ? 0 : destPrefix.length();
    }

    public static final String AWS_ENDPOINT = "AWS_ENDPOINT";

    public static final String USAGE_ENDPOINT = "AWS endpoint to use (or set " + AWS_ENDPOINT + " in your environment)";
    public static final String OPT_ENDPOINT = "-e";
    public static final String LONGOPT_ENDPOINT = "--endpoint";
    @Option(name = OPT_ENDPOINT, aliases = LONGOPT_ENDPOINT, usage = USAGE_ENDPOINT)
    @Getter
    @Setter
    private String endpoint = System.getenv().get(AWS_ENDPOINT);

    public boolean hasEndpoint() {
        return endpoint != null && endpoint.trim().length() > 0;
    }

    public static final String USAGE_MAX_CONNECTIONS = "Maximum number of connections to S3 (default 100)";
    public static final String OPT_MAX_CONNECTIONS = "-m";
    public static final String LONGOPT_MAX_CONNECTIONS = "--max-connections";
    @Option(name = OPT_MAX_CONNECTIONS, aliases = LONGOPT_MAX_CONNECTIONS, usage = USAGE_MAX_CONNECTIONS)
    @Getter
    @Setter
    private int maxConnections = 100;

    public static final String USAGE_MAX_THREADS = "Maximum number of threads (default 100)";
    public static final String OPT_MAX_THREADS = "-t";
    public static final String LONGOPT_MAX_THREADS = "--max-threads";
    @Option(name = OPT_MAX_THREADS, aliases = LONGOPT_MAX_THREADS, usage = USAGE_MAX_THREADS)
    @Getter
    @Setter
    private int maxThreads = 100;

    public static final String USAGE_MAX_RETRIES = "Maximum number of retries for S3 requests (default 5)";
    public static final String OPT_MAX_RETRIES = "-r";
    public static final String LONGOPT_MAX_RETRIES = "--max-retries";
    @Option(name = OPT_MAX_RETRIES, aliases = LONGOPT_MAX_RETRIES, usage = USAGE_MAX_RETRIES)
    @Getter
    @Setter
    private int maxRetries = 5;

    public static final String USAGE_CTIME = "Only copy objects whose Last-Modified date is younger than this many days. " +
            "For other time units, use these suffixes: y (years), M (months), d (days), w (weeks), h (hours), m (minutes), s (seconds)";
    public static final String OPT_CTIME = "-c";
    public static final String LONGOPT_CTIME = "--ctime";
    @Option(name = OPT_CTIME, aliases = LONGOPT_CTIME, usage = USAGE_CTIME)
    @Getter
    @Setter
    private String ctime = null;

    public boolean hasCtime() {
        return ctime != null;
    }

    private static final String PROXY_USAGE = "host:port of proxy server to use. " +
            "Defaults to proxy_host and proxy_port defined in ~/.s3cfg, or no proxy if these values are not found in ~/.s3cfg";
    public static final String OPT_PROXY = "-z";
    public static final String LONGOPT_PROXY = "--proxy";

    @Option(name = OPT_PROXY, aliases = LONGOPT_PROXY, usage = PROXY_USAGE)
    public void setProxy(String proxy) {
        final String[] splits = proxy.split(":");
        if (splits.length != 2) {
            throw new IllegalArgumentException("Invalid proxy setting (" + proxy + "), please use host:port");
        }

        proxyHost = splits[0];
        if (proxyHost.trim().length() == 0) {
            throw new IllegalArgumentException("Invalid proxy setting (" + proxy + "), please use host:port");
        }
        try {
            proxyPort = Integer.parseInt(splits[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid proxy setting (" + proxy + "), port could not be parsed as a number");
        }
    }

    @Getter
    @Setter
    public String proxyHost = null;
    @Getter
    @Setter
    public int proxyPort = -1;

    public boolean getHasProxy() {
        boolean hasProxyHost = proxyHost != null && proxyHost.trim().length() > 0;
        boolean hasProxyPort = proxyPort != -1;
        return hasProxyHost && hasProxyPort;
    }

    private long initMaxAge() {

        DateTime dateTime = new DateTime(nowTime);

        // all digits -- assume "days"
        if (ctime.matches("^[0-9]+$")) return dateTime.minusDays(Integer.parseInt(ctime)).getMillis();

        // ensure there is at least one digit, and exactly one character suffix, and the suffix is a legal option
        if (!ctime.matches("^[0-9]+[yMwdhms]$"))
            throw new IllegalArgumentException("Invalid option for ctime: " + ctime);

        if (ctime.endsWith("y")) return dateTime.minusYears(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("M")) return dateTime.minusMonths(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("w")) return dateTime.minusWeeks(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("d")) return dateTime.minusDays(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("h")) return dateTime.minusHours(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("m")) return dateTime.minusMinutes(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("s")) return dateTime.minusSeconds(getCtimeNumber(ctime)).getMillis();
        throw new IllegalArgumentException("Invalid option for ctime: " + ctime);
    }

    private int getCtimeNumber(String ctime) {
        return Integer.parseInt(ctime.substring(0, ctime.length() - 1));
    }

    @Getter
    private long nowTime = System.currentTimeMillis();
    @Getter
    private long maxAge;
    @Getter
    private String maxAgeDate;

    public static final String USAGE_DELETE_REMOVED = "Delete objects from the destination bucket if they do not exist in the source bucket";
    public static final String OPT_DELETE_REMOVED = "-X";
    public static final String LONGOPT_DELETE_REMOVED = "--delete-removed";
    @Option(name = OPT_DELETE_REMOVED, aliases = LONGOPT_DELETE_REMOVED, usage = USAGE_DELETE_REMOVED)
    @Getter
    @Setter
    private boolean deleteRemoved = false;

    public static final String USAGE_SOURCE_BUCKET = "source bucket[/source/prefix]";
    public static final String OPT_SOURCE_BUCKET = "-F";
    public static final String LONGOPT_SOURCE_BUCKET = "--source_bucket";
    @Option(name = OPT_SOURCE_BUCKET, aliases = LONGOPT_SOURCE_BUCKET, usage = USAGE_SOURCE_BUCKET, required = true)
    @Getter
    @Setter
    private String source;

    public static final String USAGE_DESTINATION_BUCKET = "destination bucket[/destination/prefix]";
    public static final String OPT_DESTINATION_BUCKET = "-T";
    public static final String LONGOPT_DESTINATION_BUCKET = "--destination_bucket";
    @Option(name = OPT_DESTINATION_BUCKET, aliases = LONGOPT_DESTINATION_BUCKET, usage = USAGE_DESTINATION_BUCKET, required = true)
    @Getter
    @Setter
    private String destination;


    @Getter
    private String sourceBucket;

    @Getter
    private String destinationBucket;


    /**
     * Current max file size allowed in amazon is 5 GB. We can try and provide this as an option too.
     */
    public static final long MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE = 5 * MirrorConstants.GB;
    private static final long DEFAULT_PART_SIZE = 4 * MirrorConstants.GB;
    private static final String MULTI_PART_UPLOAD_SIZE_USAGE = "The upload size (in bytes) of each part uploaded as part of a multipart request " +
            "for files that are greater than the max allowed file size of " + MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE + " bytes (" + (MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE / MirrorConstants.GB) + "GB). " +
            "Defaults to " + DEFAULT_PART_SIZE + " bytes (" + (DEFAULT_PART_SIZE / MirrorConstants.GB) + "GB).";
    private static final String OPT_MULTI_PART_UPLOAD_SIZE = "-u";
    private static final String LONGOPT_MULTI_PART_UPLOAD_SIZE = "--upload-part-size";
    @Option(name = OPT_MULTI_PART_UPLOAD_SIZE, aliases = LONGOPT_MULTI_PART_UPLOAD_SIZE, usage = MULTI_PART_UPLOAD_SIZE_USAGE)
    @Getter
    @Setter
    private long uploadPartSize = DEFAULT_PART_SIZE;

    private static final String CROSS_ACCOUNT_USAGE = "Copy across AWS accounts. Only Resource-based policies are supported (as " +
            "specified by AWS documentation) for cross account copying. " +
            "Default is false (copying within same account, preserving ACLs across copies). " +
            "If this option is active, we give full access to owner of the destination bucket.";
    private static final String OPT_CROSS_ACCOUNT_COPY = "-C";
    private static final String LONGOPT_CROSS_ACCOUNT_COPY = "--cross-account-copy";
    @Option(name = OPT_CROSS_ACCOUNT_COPY, aliases = LONGOPT_CROSS_ACCOUNT_COPY, usage = CROSS_ACCOUNT_USAGE)
    @Getter
    @Setter
    private boolean crossAccountCopy = false;

    public static final String USAGE_SRC_STORE = "Source storage type (only 'S3' is supported, for current version. Source store will be default to 'S3' if not specified)";
    public static final String OPT_SRC_STORE = "-S";
    public static final String LONGOPT_SRC_STORE = "--src-store";
    @Option(name = OPT_SRC_STORE, aliases = LONGOPT_SRC_STORE, usage = USAGE_SRC_STORE)
    @Getter
    @Setter
    private String srcStore = "S3";

    public static final String USAGE_DEST_STORE = "Destination storage type [S3|GCS]. Destination store will be default to 'S3' if not specified)";
    public static final String OPT_DEST_STORE = "-D";
    public static final String LONGOPT_DEST_STORE = "--dest-store";
    @Option(name = OPT_DEST_STORE, aliases = LONGOPT_DEST_STORE, usage = USAGE_DEST_STORE)
    @Getter
    @Setter
    private String destStore = "S3";


    public static final String USAGE_GCS_APPLICAION_NAME = "Be sure to specify the name of your application. If the application name is null or blank, the application will log a warning. Suggested format is \"MyCompany-ProductName/1.0\".";
    public static final String OPT_GCS_APPLICAION_NAME = "-A";
    public static final String LONGOPT_GCS_APPLICAION_NAME = "--gcs-application-name";
    @Option(name = OPT_GCS_APPLICAION_NAME, aliases = LONGOPT_GCS_APPLICAION_NAME, usage = USAGE_GCS_APPLICAION_NAME)
    @Getter
    @Setter
    private String GCS_APPLICATION_NAME;


    public void initDerivedFields() {

        if (hasCtime()) {
            this.maxAge = initMaxAge();
            this.maxAgeDate = new Date(maxAge).toString();
        }

        String scrubbed;
        int slashPos;

        scrubbed = scrubS3ProtocolPrefix(source);
        slashPos = scrubbed.indexOf(MirrorConstants.SLASH);
        if (slashPos == -1) {
            sourceBucket = scrubbed;
        } else {
            sourceBucket = scrubbed.substring(0, slashPos);
            if (hasPrefix()) {
                throw new IllegalArgumentException("Cannot use a " + OPT_PREFIX + "/" + LONGOPT_PREFIX + " argument and source path that includes a prefix at the same time");
            }
            prefix = scrubbed.substring(slashPos + 1);
        }

        scrubbed = scrubS3ProtocolPrefix(destination);
        slashPos = scrubbed.indexOf('/');
        if (slashPos == -1) {
            destinationBucket = scrubbed;
        } else {
            destinationBucket = scrubbed.substring(0, slashPos);
            if (hasDestPrefix()) {
                throw new IllegalArgumentException("Cannot use a " + OPT_DEST_PREFIX + "/" + LONGOPT_DEST_PREFIX + " argument and destination path that includes a dest-prefix at the same time");
            }
            destPrefix = scrubbed.substring(slashPos + 1);
        }
    }

    protected String scrubS3ProtocolPrefix(String bucket) {
        bucket = bucket.trim();
        if (bucket.startsWith(MirrorConstants.S3_PROTOCOL_PREFIX)) {
            bucket = bucket.substring(MirrorConstants.S3_PROTOCOL_PREFIX.length());
        }
        return bucket;
    }

}
