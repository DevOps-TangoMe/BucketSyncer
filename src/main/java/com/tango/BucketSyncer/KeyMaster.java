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

import com.tango.BucketSyncer.KeyJobs.KeyJob;
import com.tango.BucketSyncer.KeyListers.KeyLister;

import com.tango.BucketSyncer.ObjectSummaries.ObjectSummary;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public abstract class KeyMaster implements Runnable {

    public static final int STOP_TIMEOUT_SECONDS = 10;
    private static final long STOP_TIMEOUT = TimeUnit.SECONDS.toMillis(STOP_TIMEOUT_SECONDS);

    protected Object sourceClient;
    protected Object destClient;
    protected MirrorContext context;

    private AtomicBoolean done = new AtomicBoolean(false);

    public boolean isDone() {
        return done.get();
    }

    private BlockingQueue<Runnable> workQueue;
    private ThreadPoolExecutor executorService;
    protected final Object notifyLock = new Object();

    private Thread thread;


    public KeyMaster(Object sourceClient,
                     Object destClient,
                     MirrorContext context,
                     BlockingQueue<Runnable> workQueue,
                     ThreadPoolExecutor executorService) {
        this.sourceClient = sourceClient;
        this.destClient = destClient;
        this.context = context;
        this.workQueue = workQueue;
        this.executorService = executorService;
    }


    protected abstract KeyLister getKeyLister(MirrorOptions options);

    protected abstract KeyJob getTask(ObjectSummary summary);

    public void start() {
        this.thread = new Thread(this);
        this.thread.start();
    }

    public void stop() {
        final String name = getClass().getSimpleName();
        final long start = System.currentTimeMillis();
        log.info("stopping {}...", name);

        try {
            if (isDone())
                return;

            this.thread.interrupt();

            while (!isDone() && System.currentTimeMillis() - start < STOP_TIMEOUT) {
                if (Sleep.sleep(50))
                    return;
            }
        } finally {
            if (!isDone()) {
                try {
                    log.warn("{} didn't stop within {} after interrupting it, forcibly killing the thread...", name, STOP_TIMEOUT_SECONDS);
                    this.thread.stop();
                } catch (Exception e) {
                    log.error("Error calling Thread.stop on {}: {}", name, e);
                }
            }
            if (isDone())
                log.info("{} stopped", name);
        }
    }

    public void run() {

        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();

        final int maxQueueCapacity = MirrorMaster.getMaxQueueCapacity(options);

        int counter = 0;
        try {
            final KeyLister lister = getKeyLister(options);
            executorService.submit(lister);

            List<ObjectSummary> summaries = lister.getNextBatch();
            if (verbose) {
                log.info("{} keys found in first batch from source bucket -- processing...", summaries.size());
            }

            while (true) {
                for (ObjectSummary summary : summaries) {
                    while (workQueue.size() >= maxQueueCapacity) {
                        try {
                            synchronized (notifyLock) {
                                notifyLock.wait(50);
                            }
                            Thread.sleep(50);

                        } catch (InterruptedException e) {
                            log.error("interrupted!");
                            return;
                        }
                    }
                    executorService.submit(getTask(summary));
                    counter++;
                }

                summaries = lister.getNextBatch();
                if (summaries.size() > 0) {
                    if (verbose) {
                        log.info("{} more keys found in source bucket -- continuing (queue size = {}, total processed = {})...", new Object[]{summaries.size(), workQueue.size(), counter});
                    }

                } else if (lister.isDone()) {
                    if (verbose) {
                        log.info("No more keys found in source bucket -- ALL DONE");
                    }
                    return;

                } else {
                    if (verbose) {
                        log.info("Lister has no keys queued, but is not done, waiting and retrying");
                    }
                    if (Sleep.sleep(50)) return;
                }
            }

        } catch (Exception e) {
            log.error("Unexpected exception in MirrorMaster: ", e);

        } finally {
            while (workQueue.size() > 0 || executorService.getActiveCount() > 0) {
                // wait for the queue to be empty
                if (Sleep.sleep(100)) break;
            }
            // this will wait for currently executing tasks to finish
            executorService.shutdown();
            done.set(true);
        }
    }
}
