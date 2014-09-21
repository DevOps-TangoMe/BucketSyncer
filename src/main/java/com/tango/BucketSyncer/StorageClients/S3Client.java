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
package com.tango.BucketSyncer.StorageClients;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3Client;
import com.tango.BucketSyncer.MirrorOptions;

public class S3Client implements StorageClient {
    private AmazonS3Client s3Client = null;

    public void createClient(MirrorOptions options) {
        ClientConfiguration clientConfiguration = new ClientConfiguration().withProtocol(Protocol.HTTP)
                .withMaxConnections(options.getMaxConnections());
        if (options.getHasProxy()) {
            clientConfiguration = clientConfiguration
                    .withProxyHost(options.getProxyHost())
                    .withProxyPort(options.getProxyPort());
        }
        this.s3Client = new AmazonS3Client(options, clientConfiguration);
        if (options.hasEndpoint()) {
            s3Client.setEndpoint(options.getEndpoint());
        }
    }

    @Override
    public Object getClient(MirrorOptions options) {
        createClient(options);
        return this.s3Client;
    }

}
