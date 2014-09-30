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

import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.*;

/**
 * Manages the Starts a KEY_LISTER and sends batches of keys to the ExecutorService for handling by KeyJobs
 */
@Slf4j
public class MirrorMaster {

    public static final String VERSION = System.getProperty("BucketSyncer.version");

    private final MirrorContext context;
    private final Object sourceClient;
    private final Object destClient;

    public MirrorMaster(Object sourceClient, Object destClient, MirrorContext context) {
        this.sourceClient = sourceClient;
        this.destClient = destClient;
        this.context = context;
    }

    public void mirror() {

        log.info("version " + VERSION + " starting");

        final MirrorOptions options = context.getOptions();

        if (options.isVerbose() && options.hasCtime()) {
            log.info("will not copy anything older than {} (cutoff = {})", options.getCtime(), options.getMaxAgeDate());
        }

        final int maxQueueCapacity = getMaxQueueCapacity(options);
        final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(maxQueueCapacity);
        final RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                log.error("Error submitting job: {}, possible queue overflow", r);
            }
        };

        final ThreadPoolExecutor executorService = new ThreadPoolExecutor(options.getMaxThreads(),
                options.getMaxThreads(),
                1,
                TimeUnit.MINUTES,
                workQueue,
                rejectedExecutionHandler);


        final KeyMaster copyMaster = new CopyMaster(sourceClient, destClient, workQueue, executorService, context);


        KeyMaster deleteMaster = null;

        try {
            copyMaster.start();

            if (context.getOptions().isDeleteRemoved()) {
                deleteMaster = new DeleteMaster(sourceClient, destClient, context, workQueue, executorService);
                deleteMaster.start();
            }

            while (true) {
                if (copyMaster.isDone() && (deleteMaster == null || deleteMaster.isDone())) {
                    log.info("mirror: completed");
                    break;
                }

                // sleep 10 seconds before re-checking the status
                if (Sleep.sleep(10000))
                    return;
            }

        } catch (Exception e) {
            log.error("Unexpected exception in mirror: ", e);
        } finally {
            try {
                copyMaster.stop();
            } catch (Exception e) {
                log.error("Error stopping copyMaster: ", e);
            }

            if (deleteMaster != null) {
                try {
                    deleteMaster.stop();
                } catch (Exception e) {
                    log.error("Error stopping deleteMaster: ", e);
                }
            }
        }
    }

    public static int getMaxQueueCapacity(MirrorOptions options) {
        return 10 * options.getMaxThreads();
    }

}
