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

import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.tango.BucketSyncer.StorageClients.StorageClient;
import lombok.Cleanup;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.CmdLineParser;

import java.io.*;

/**
 * Provides the "main" method. Responsible for parsing options and setting up the MirrorMaster to manage the copy.
 */
@Slf4j
public class MirrorMain {

    @Getter
    @Setter
    private String[] args;

    @Getter
    private final MirrorOptions options = new MirrorOptions();

    private final CmdLineParser parser = new CmdLineParser(options);


    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            log.error("Uncaught Exception (thread {}): ", t.getName(), e);
        }
    };

    @Getter
    @VisibleForTesting
    @Setter
    private MirrorContext context;
    @Getter
    @VisibleForTesting
    @Setter
    private MirrorMaster master;

    @Getter
    @VisibleForTesting
    @Setter
    private Object sourceClient;
    @Getter
    @VisibleForTesting
    @Setter
    private Object destClient;


    public MirrorMain(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) {
        MirrorMain main = new MirrorMain(args);
        main.run();
    }

    public void run() {
        init();
        master.mirror();
    }

    public void init() {
        if (sourceClient == null) {
            try {
                parseArguments();
            } catch (Exception e) {
                log.error("Error in parsing arguments: ", e);
                parser.printUsage(System.err);
                System.exit(1);
            }

            sourceClient = getSourceClient(options);
            context = new MirrorContext(options);

            destClient = getDestClient(options);
            master = new MirrorMaster(sourceClient, destClient, context);

            Runtime.getRuntime().addShutdownHook(context.getStats().getShutdownHook());
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        }

    }


    protected void parseArguments() throws Exception {
        parser.parseArgument(args);
        if (!options.hasAwsKeys()) {
            // try to load from src/main/resources/s3cfg.properties
            @Cleanup BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/s3cfg.properties"));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().startsWith("access_key")) {
                    options.setAWSAccessKeyId(line.substring(line.indexOf("=") + 1).trim());
                } else if (line.trim().startsWith("secret_key")) {
                    options.setAWSSecretKey(line.substring(line.indexOf("=") + 1).trim());
                } else if (!options.getHasProxy() && line.trim().startsWith("proxy_host")) {
                    options.setProxyHost(line.substring(line.indexOf("=") + 1).trim());
                } else if (!options.getHasProxy() && line.trim().startsWith("proxy_port")){
                    options.setProxyPort(Integer.parseInt(line.substring(line.indexOf("=") + 1).trim()));
                }
            }
        }

        if (!options.hasAwsKeys()) {
            throw new IllegalStateException("ENV vars not defined: " + MirrorOptions.AWS_ACCESS_KEY + " and/or " + MirrorOptions.AWS_SECRET_KEY);
        }

        // sanity check for destination store and source store
        // problem with the sanity check when sync to GCS

//        if (options.getDestStore() != "S3" && options.getDestStore() != "GCS") {
//            log.warn("Current DestStore: {}", options.getDestStore());
//            throw new IllegalArgumentException("Destination store should be one of [S3|GCS]");
//        }
//        if (options.getSrcStore() != "S3") {
//            log.warn("Current SrcStore: {}", options.getSrcStore());
//            throw new IllegalArgumentException("Only S3 is supported for source store!");
//        }
        //need to have GCS_APPLICATION_NAME if sync from or to GCS
//        if (options.getDestStore() == "GCS" || options.getSrcStore() == "GCS") {
//            if (options.getGCS_APPLICATION_NAME() == null) {
//                throw new IllegalArgumentException("Please provide GCS application name");
//            }
//        }

        options.initDerivedFields();
    }

    protected Object getSourceClient(MirrorOptions options) {

        String name = options.getSrcStore().toString().toUpperCase();
        String clientName = name + "Client";
        String packageName = this.getClass().getPackage().getName();
        String className = String.format("%s.StorageClients.%s", packageName, clientName);

        Class<?> clazz = null;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            log.error("Classname for SourceClient is not found. It is possible that this storage client has not been implemented as SourceClient: ", e);
            Throwables.propagate(e);
        }
        StorageClient client = null;
        try {
            client = (StorageClient) clazz.newInstance();
        } catch (InstantiationException e) {
            log.error("Failed to instantiate SourceClient: ", e);
            Throwables.propagate(e);
        } catch (IllegalAccessException e) {
            log.error("Failed to access SourceClient: ", e);
            Throwables.propagate(e);
        }
        return client.getClient(options);
    }

    protected Object getDestClient(MirrorOptions options) {
        String name = options.getDestStore().toString().toUpperCase();
        String clientName = name + "Client";
        String packageName = this.getClass().getPackage().getName();
        String className = String.format("%s.StorageClients.%s", packageName, clientName);
        Class<?> clazz = null;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            log.error("Classname for DestClient is not found. It is possible that this storage client has not been implemented as DestClient: ", e);
            Throwables.propagate(e);
        }
        StorageClient client = null;
        try {
            client = (StorageClient) clazz.newInstance();
        } catch (InstantiationException e) {
            log.error("Classname for instantiate is not found: ", e);
            Throwables.propagate(e);
        } catch (IllegalAccessException e) {
            log.error("Classname for access is not found: ", e);
            Throwables.propagate(e);
        }
        return client.getClient(options);
    }

}
