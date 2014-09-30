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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

@Slf4j
public class DeleteMaster extends KeyMaster {

    public DeleteMaster(Object sourceClient, Object destClient, MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executorService) {
        super(sourceClient, destClient, context, workQueue, executorService);
    }

    protected String getPrefix(MirrorOptions options) {
        return options.hasDestPrefix() ? options.getDestPrefix() : options.getPrefix();
    }

    protected String getBucket(MirrorOptions options) {
        return options.getDestinationBucket();
    }


    protected KeyLister getKeyLister(MirrorOptions options) {
        String packageName = this.getClass().getPackage().getName();
        String name = context.getOptions().getDestStore().toString().toUpperCase();
        String listerName = String.format("%s%s", name, MirrorConstants.KEY_LISTER);
        String className = String.format("%s.%s.%s", packageName, MirrorConstants.KEY_LISTERS, listerName);

        Class<? extends KeyLister> clazz = null;
        try {
            clazz = (Class<? extends KeyLister>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            log.error("Classname for KEY_LISTER {} is not found: {}", className, e);
        }
        Constructor<?> constructor = null;
        try {
            constructor = clazz.getConstructor(Object.class,
                    String.class,
                    String.class,
                    MirrorContext.class,
                    Integer.class);
        } catch (NoSuchMethodException e) {
            log.error("Failed to find corresponding KEY_LISTER constructor: ", e);
        }
        try {
            return (KeyLister) constructor
                    .newInstance(destClient,
                            getBucket(options),
                            getPrefix(options),
                            context,
                            MirrorMaster.getMaxQueueCapacity(options));
        } catch (InstantiationException e) {
            log.error("Failed to instantiate KEY_LISTER: ", e);
        } catch (IllegalAccessException e) {
            log.error("Failed to access KEY_LISTER: ", e);
        } catch (InvocationTargetException e) {
            log.error("Failed to invocate KEY_LISTER: ", e);
        }
        return null;

    }

    protected KeyJob getTask(ObjectSummary summary) {
        String src = context.getOptions().getSrcStore().toString().toUpperCase();
        String dest = context.getOptions().getDestStore().toString().toUpperCase();
        String keyDeleteJobName = String.format("%s2%s%s", src, dest, MirrorConstants.KEY_DELETE_JOB);
        String packageName = this.getClass().getPackage().getName();
        String className = String.format("%s.%s.%s", packageName, MirrorConstants.KEY_JOBS, keyDeleteJobName);

        Class<?> clazz = null;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            log.error("Classname for KeyDeleteJobs {} is not found: {}", className, e);
        }
        Constructor<?> constructor = null;
        try {
            constructor = clazz.getConstructor(Object.class,
                    Object.class,
                    MirrorContext.class,
                    ObjectSummary.class,
                    Object.class);
        } catch (NoSuchMethodException e) {
            log.error("Failed to find corresponding KeyDeleteJob {} constructor: {}", className, e);
        }
        try {
            return (KeyJob) constructor.newInstance(sourceClient,
                    destClient,
                    context,
                    summary,
                    notifyLock);
        } catch (InstantiationException e) {
            log.error("Failed to instantiate KeyDeleteJobs: ", e);
        } catch (IllegalAccessException e) {
            log.error("Failed to access KeyDeleteJobs: ", e);
        } catch (InvocationTargetException e) {
            log.error("Failed to invocate KeyDeleteJobs: ", e);
        }
        return null;
    }

}
