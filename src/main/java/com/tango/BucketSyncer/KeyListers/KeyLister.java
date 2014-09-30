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
package com.tango.BucketSyncer.KeyListers;

import com.tango.BucketSyncer.MirrorContext;
import com.tango.BucketSyncer.ObjectSummaries.ObjectSummary;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public abstract class KeyLister implements Runnable {
    protected MirrorContext context;
    protected Integer maxQueueCapacity;
    protected String bucket;
    protected String prefix;

    protected final AtomicBoolean done = new AtomicBoolean(false);

    public boolean isDone() {
        return done.get();
    }

    public KeyLister(String bucket,
                     String prefix,
                     MirrorContext context,
                     Integer maxQueueCapacity) {
        this.bucket = bucket;
        this.prefix = prefix;
        this.context = context;
        this.maxQueueCapacity = maxQueueCapacity;
    }

    public abstract List<ObjectSummary> getNextBatch();

}