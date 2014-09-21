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

import com.tango.BucketSyncer.KeyJobs.KeyJob;
import com.tango.BucketSyncer.KeyListers.KeyLister;
import com.tango.BucketSyncer.ObjectSummaries.ObjectSummary;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

@Slf4j
public class CopyMaster extends KeyMaster {


    public CopyMaster(Object sourceClient, Object destClient, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executorService, MirrorContext context) {
        super(sourceClient, destClient, context, workQueue, executorService);
    }


    protected String getPrefix(MirrorOptions options) {
        return options.getPrefix();
    }

    protected String getBucket(MirrorOptions options) {
        return options.getSourceBucket();
    }


    protected KeyLister getKeyLister(MirrorOptions options) {

        String packageName = this.getClass().getPackage().getName();
        String name = context.getOptions().getSrcStore().toString().toUpperCase();
        String listerName = name + "KeyLister";
        String classname = String.format("%s.KeyListers.%s", packageName, listerName);


        Class<? extends KeyLister> clazz = null;
        try {
            clazz = (Class<? extends KeyLister>) Class.forName(classname);
        } catch (ClassNotFoundException e) {
            log.error("Classname for KeyLister is not found: ", e);
        }
        Constructor<?> constructor = null;
        try {
            constructor = clazz.getConstructor(Object.class,
                    String.class,
                    String.class,
                    MirrorContext.class,
                    Integer.class);
        } catch (NoSuchMethodException e) {
            log.error("Failed to find corresponding KeyLister constructor", e);
        }
        try {
            return (KeyLister) constructor.newInstance(sourceClient, getBucket(options),
                    getPrefix(options),
                    context,
                    MirrorMaster.getMaxQueueCapacity(options));
        } catch (InstantiationException e) {
            log.error("Failed to instantiate KeyLister", e);
        } catch (IllegalAccessException e) {
            log.error("Failed to access KeyLister", e);
        } catch (InvocationTargetException e) {
            log.error("Failed to invocate KeyLister", e);
        }
        return null;
    }


    protected KeyJob getTask(ObjectSummary summary) {
        String packageName = this.getClass().getPackage().getName();
        String src = context.getOptions().getSrcStore().toString().toUpperCase();
        String dest = context.getOptions().getDestStore().toString().toUpperCase();
        String keyCopyJobName = String.format("%s2%sKeyCopyJob", src, dest);
        String multipartKeyCopyJobName = String.format("%s2%sMultipartKeyCopyJob", src, dest);
        String classname;

        //currently, only S3 supports multipartupload.
        if (summary.getSize() > MirrorOptions.MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE && dest == "S3") {
            classname = String.format("%s.KeyJobs.%s", packageName, multipartKeyCopyJobName);
        } else {
            classname = String.format("%s.KeyJobs.%s", packageName, keyCopyJobName);
        }

        Class<?> clazz = null;
        try {
            clazz = Class.forName(classname);
        } catch (ClassNotFoundException e) {
            log.error("Classname for KeyCopyJobs is not found: ", e);
        }
        Constructor<?> constructor = null;
        try {
            constructor = clazz.getConstructor(Object.class,
                    Object.class,
                    MirrorContext.class,
                    ObjectSummary.class,
                    Object.class);
        } catch (NoSuchMethodException e) {
            log.error("Failed to find corresponding KeyCopyJobs constructor: ", e);
        }
        try {
            return (KeyJob) constructor.newInstance(sourceClient,
                    destClient,
                    context,
                    summary,
                    notifyLock);
        } catch (InstantiationException e) {
            log.error("Failed to instantiate KeyCopyJobs: ", e);
        } catch (IllegalAccessException e) {
            log.error("Failed to access KeyCopyJobs: ", e);
        } catch (InvocationTargetException e) {
            log.error("Failed to invocate KeyCopyJobs: ", e);
        }
        return null;
    }
}
