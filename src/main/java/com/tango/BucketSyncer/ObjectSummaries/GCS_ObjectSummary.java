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
package com.tango.BucketSyncer.ObjectSummaries;

import com.google.api.services.storage.model.StorageObject;

import java.util.Date;

public class GCS_ObjectSummary implements ObjectSummary {
    StorageObject storageObject;

    public GCS_ObjectSummary(StorageObject storageObject) {
        this.storageObject = storageObject;
    }

    @Override
    public String getKey() {
        return storageObject.getName();
    }

    @Override
    public long getSize() {
        return storageObject.getSize().longValue();
    }

    @Override
    public Date getLastModified() {
        return new Date(storageObject.getUpdated().getValue());
    }

    @Override
    public String getETag() {
        return storageObject.getEtag();
    }
}
