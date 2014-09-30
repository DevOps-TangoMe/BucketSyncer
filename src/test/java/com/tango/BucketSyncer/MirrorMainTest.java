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

import org.junit.Test;

import static org.junit.Assert.*;

public class MirrorMainTest {

    public static final String SOURCE = "s3://from-bucket";
    public static final String DESTINATION = "s3://to-bucket";

    @Test
    public void testBasicArgs() throws Exception {

        final MirrorMain main = new MirrorMain(new String[]{MirrorOptions.OPT_SOURCE_BUCKET, SOURCE, MirrorOptions.OPT_DESTINATION_BUCKET, DESTINATION});
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertFalse(options.isDryRun());
        assertEquals(SOURCE, options.getSource());
        assertEquals(DESTINATION, options.getDestination());
    }

    @Test
    public void testDryRunArgs() throws Exception {

        final MirrorMain main = new MirrorMain(new String[]{MirrorOptions.OPT_DRY_RUN, MirrorOptions.OPT_SOURCE_BUCKET, SOURCE, MirrorOptions.OPT_DESTINATION_BUCKET, DESTINATION});
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertTrue(options.isDryRun());
        assertEquals(SOURCE, options.getSource());
        assertEquals(DESTINATION, options.getDestination());
    }

    @Test
    public void testMaxConnectionsArgs() throws Exception {

        int maxConns = 42;
        final MirrorMain main = new MirrorMain(new String[]{MirrorOptions.OPT_MAX_CONNECTIONS, String.valueOf(maxConns), MirrorOptions.OPT_SOURCE_BUCKET, SOURCE, MirrorOptions.OPT_DESTINATION_BUCKET, DESTINATION});
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertFalse(options.isDryRun());
        assertEquals(maxConns, options.getMaxConnections());
        assertEquals(SOURCE, options.getSource());
        assertEquals(DESTINATION, options.getDestination());
    }

    @Test
    public void testInlinePrefix() throws Exception {
        final String prefix = "foo";
        final MirrorMain main = new MirrorMain(new String[]{MirrorOptions.OPT_SOURCE_BUCKET, SOURCE + "/" + prefix, MirrorOptions.OPT_DESTINATION_BUCKET, DESTINATION});
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertEquals(prefix, options.getPrefix());
        assertNull(options.getDestPrefix());
    }

    @Test
    public void testInlineDestPrefix() throws Exception {
        final String destPrefix = "foo";
        final MirrorMain main = new MirrorMain(new String[]{MirrorOptions.OPT_SOURCE_BUCKET, SOURCE, MirrorOptions.OPT_DESTINATION_BUCKET, DESTINATION + "/" + destPrefix});
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertEquals(destPrefix, options.getDestPrefix());
        assertNull(options.getPrefix());
    }

    @Test
    public void testInlineSourceAndDestPrefix() throws Exception {
        final String prefix = "foo";
        final String destPrefix = "bar";
        final MirrorMain main = new MirrorMain(new String[]{MirrorOptions.OPT_SOURCE_BUCKET, SOURCE + "/" + prefix, MirrorOptions.OPT_DESTINATION_BUCKET, DESTINATION + "/" + destPrefix});
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertEquals(prefix, options.getPrefix());
        assertEquals(destPrefix, options.getDestPrefix());
    }

    @Test
    public void testInlineSourcePrefixAndPrefixOption() throws Exception {
        final String prefix = "foo";
        final MirrorMain main = new MirrorMain(new String[]{MirrorOptions.OPT_PREFIX, prefix, MirrorOptions.OPT_SOURCE_BUCKET, SOURCE + "/" + prefix, MirrorOptions.OPT_DESTINATION_BUCKET, DESTINATION});
        try {
            main.parseArguments();
            fail("expected IllegalArgumentException");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testInlineDestinationPrefixAndPrefixOption() throws Exception {
        final String prefix = "foo";
        final MirrorMain main = new MirrorMain(new String[]{MirrorOptions.OPT_DEST_PREFIX, prefix, MirrorOptions.OPT_SOURCE_BUCKET, SOURCE, MirrorOptions.OPT_DESTINATION_BUCKET, DESTINATION + "/" + prefix});
        try {
            main.parseArguments();
            fail("expected IllegalArgumentException");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    /**
     * When access keys are read from environment then the --proxy setting is valid.
     * If access keys are ready from s3cfg file then proxy settings are picked from there.
     *
     * @throws Exception
     */
    @Test
    public void testProxyHostAndProxyPortOption() throws Exception {
        final String proxy = "localhost:8080";
        final MirrorMain main = new MirrorMain(new String[]{MirrorOptions.OPT_PROXY, proxy, MirrorOptions.OPT_SOURCE_BUCKET, SOURCE, MirrorOptions.OPT_DESTINATION_BUCKET, DESTINATION});

        main.getOptions().setAWSAccessKeyId("accessKey");
        main.getOptions().setAWSSecretKey("secretKey");
        main.parseArguments();
        assertEquals("localhost", main.getOptions().getProxyHost());
        assertEquals(8080, main.getOptions().getProxyPort());
    }

    @Test
    public void testInvalidProxyOption() throws Exception {
        for (String proxy : new String[]{"localhost", "localhost:", ":1234", "localhost:invalid", ":", ""}) {
            testInvalidProxySetting(proxy);
        }
    }

    @Test
    public void testSrcStoreAndDestStoreOption() throws Exception {
        final String srcStore = "S3";
        final String destStore = "GCS";
        final String gcsAppName = "GCS_APP";
        final MirrorMain main = new MirrorMain(new String[]{MirrorOptions.OPT_SRC_STORE, srcStore,
                MirrorOptions.OPT_DEST_STORE, destStore,
                MirrorOptions.OPT_SOURCE_BUCKET, SOURCE, MirrorOptions.OPT_DESTINATION_BUCKET, DESTINATION, MirrorOptions.OPT_GCS_APPLICAION_NAME, gcsAppName});
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertEquals(srcStore, options.getSrcStore());
        assertEquals(destStore, options.getDestStore());
    }


    @Test
    public void testGCSApplicationOption() throws Exception {
        final String gcsApplication = "MyCompany-ProductName/1.0";
        final MirrorMain main = new MirrorMain(new String[]{MirrorOptions.OPT_GCS_APPLICAION_NAME, gcsApplication, MirrorOptions.OPT_SOURCE_BUCKET, SOURCE, MirrorOptions.OPT_DESTINATION_BUCKET, DESTINATION});
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertEquals(gcsApplication, options.getGCS_APPLICATION_NAME());
    }


    private void testInvalidProxySetting(String proxy) throws Exception {
        final MirrorMain main = new MirrorMain(new String[]{MirrorOptions.OPT_PROXY, proxy, MirrorOptions.OPT_SOURCE_BUCKET, SOURCE, MirrorOptions.OPT_DESTINATION_BUCKET, DESTINATION});
        main.getOptions().setAWSAccessKeyId("accessKey");
        main.getOptions().setAWSSecretKey("secretKey");
        try {
            main.parseArguments();
            fail("Invalid proxy setting (" + proxy + ") should have thrown exception");
        } catch (IllegalArgumentException expected) {
        }
    }
}
