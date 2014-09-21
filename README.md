BucketSyncer
==========

A utility for mirroring content from one S3 bucket to another S3 bucket or Google Cloud Storage bucket.

Designed to be lightning-fast and highly concurrent, with modest CPU and memory requirements.

An object will be copied if and only if at least one of the following holds true:

* The object does not exist in the destination bucket.
* The size or ETag of the object in the destination bucket are different from the size/ETag in the source bucket.

When copying, the source metadata and ACL lists are also copied to the destination object.

License
-------

This is released under Apache License v2


Motivation
----------

BucketSyncer was inspired by [s3s3mirror](https://github.com/cobbzilla/s3s3mirror)

### AWS Credentials

* BucketSyncer will first look for credentials in your system environment. If variables named AWS\_ACCESS\_KEY\_ID and AWS\_SECRET\_ACCESS\_KEY are defined, then these will be used.
* Next, it checks for s3cfg.properties file under src/main/resources. If present, the access key and secret key are read from there.
* If neither of the above is found, it will error out and refuse to run.

### Google Cloud Storage Credentials

* BucketSyncer reads Google Cloud Storage Credentials from gcscfg.json under resources directory.


### System Requirements

* Java 6 or later

### Building

    mvn package

The above command requires that Maven 3 is installed.

### Usage

    BucketSyncer.sh [options] <source-bucket>[/src-prefix/path/...] <destination-bucket>[/dest-prefix/path/...]

### Options

    VAL                             : source bucket[/source/prefix]
    VAL                             : destination bucket[/dest/prefix]
    -A (--gcs-application-name) VAL : Be sure to specify the name of your
                                      application. If the application name is null
                                      or blank, the application will log a
                                      warning. Suggested format is "MyCompany-Produ
                                      ctName/1.0".
    -C (--cross-account-copy)       : Copy across AWS accounts. Only Resource-based
                                      policies are supported (as specified by AWS
                                      documentation) for cross account copying.
                                      Default is false (copying within same
                                      account, preserving ACLs across copies). If
                                      this option is active, we give full access
                                      to owner of the destination bucket.
    -D (--dest-store) VAL           : Destination storage type [S3|GCS].
                                      Destination store will be default to 'S3' if
                                      not specified)
    -S (--src-store) VAL            : Source storage type (only 'S3' is supported,
                                      for current version. Source store will be
                                      default to 'S3' if not specified)
    -X (--delete-removed)           : Delete objects from the destination bucket
                                      if they do not exist in the source bucket
    -c (--ctime) VAL                : Only copy objects whose Last-Modified date
                                      is younger than this many days. For other
                                      time units, use these suffixes: y (years), M
                                      (months), d (days), w (weeks), h (hours), m
                                      (minutes), s (seconds)
    -d (--dest-prefix) VAL          : Destination prefix (replacing the one
                                      specified in --prefix, if any)
    -e (--endpoint) VAL             : AWS endpoint to use (or set AWS_ENDPOINT in
                                      your environment)
    -m (--max-connections) N        : Maximum number of connections to S3 (default
                                      100)
    -n (--dry-run)                  : Do not actually do anything, but show what
                                      would be done
    -p (--prefix) VAL               : Only copy objects whose keys start with this
                                      prefix
    -r (--max-retries) N            : Maximum number of retries for S3 requests
                                      (default 5)
    -t (--max-threads) N            : Maximum number of threads (default 100)
    -u (--upload-part-size) N       : The upload size (in bytes) of each part
                                      uploaded as part of a multipart request for
                                      files that are greater than the max allowed
                                      file size of 5368709120 bytes (5GB).
                                      Defaults to 4294967296 bytes (4GB).
    -v (--verbose)                  : Verbose output
    -z (--proxy) VAL                : host:port of proxy server to use. Defaults
                                      to proxy_host and proxy_port defined in
                                      src/main/resources/s3cfg.properties, or no proxy if these values are
                                      not found in s3cfg.properties 

### Examples

Copy everything from a bucket named "source" to another bucket named "dest"

    BucketSyncer.sh source dest

Copy everything from "source" to "dest", but only copy objects created or modified within the past week

    BucketSyncer.sh -c 7 source dest
    BucketSyncer.sh -c 7d source dest
    BucketSyncer.sh -c 1w source dest
    BucketSyncer.sh --ctime 1w source dest

Copy everything from "source/foo" to "dest/bar"

    BucketSyncer.sh source/foo dest/bar
    BucketSyncer.sh -p foo -d bar source dest

Copy everything from "source/foo" to "dest/bar" and delete anything in "dest/bar" that does not exist in "source/foo"

    BucketSyncer.sh -X source/foo dest/bar
    BucketSyncer.sh --delete-removed source/foo dest/bar
    BucketSyncer.sh -p foo -d bar -X source dest
    BucketSyncer.sh -p foo -d bar --delete-removed source dest

Copy within a single bucket -- copy everything from "source/foo" to "source/bar"

    BucketSyncer.sh source/foo source/bar
    BucketSyncer.sh -p foo -d bar source source
    
Copy from S3 to Google Cloud Storage bucket -- copy everything from "source/foo" to "source/bar"

     BucketSyncer.sh source/foo source/bar -S S3 -D GCS
     BucketSyncer.sh -p foo -d bar source source -S S3 -D GCS

BAD IDEA: If copying within a single bucket, do *not* put the destination below the source

    BucketSyncer.sh source/foo source/foo/subfolder
    BucketSyncer.sh -p foo -d foo/subfolder source source
*This might cause recursion and raise your AWS bill unnecessarily*

