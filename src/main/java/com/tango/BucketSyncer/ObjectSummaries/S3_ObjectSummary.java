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

import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.Date;

public class S3_ObjectSummary implements ObjectSummary {
    S3ObjectSummary s3ObjectSummary;

    public S3_ObjectSummary(S3ObjectSummary s3ObjectSummary) {
        this.s3ObjectSummary = s3ObjectSummary;
    }

    @Override
    public String getKey() {
        return s3ObjectSummary.getKey();
    }

    @Override
    public long getSize() {
        return s3ObjectSummary.getSize();
    }

    @Override
    public Date getLastModified() {
        return s3ObjectSummary.getLastModified();
    }

    @Override
    public String getETag() {
        return s3ObjectSummary.getETag();
    }
}
