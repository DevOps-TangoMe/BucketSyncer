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
package com.tango.BucketSyncer.KeyJobs;

import com.tango.BucketSyncer.MirrorContext;
import com.tango.BucketSyncer.ObjectSummaries.ObjectSummary;

public abstract class KeyJob implements Runnable {
    protected final ObjectSummary summary;
    protected final Object notifyLock;
    protected final MirrorContext context;


    public KeyJob(ObjectSummary summary,
                  Object notifyLock,
                  MirrorContext context) {
        this.summary = summary;
        this.notifyLock = notifyLock;
        this.context = context;
    }

    @Override
    public String toString() {
        return summary.toString();
    }


}
