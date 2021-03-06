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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.tango.BucketSyncer.MirrorConstants;
import com.tango.BucketSyncer.MirrorMain;
import com.tango.BucketSyncer.MirrorOptions;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.security.GeneralSecurityException;
import java.util.Collections;

@Slf4j
public class GCSClient implements StorageClient {
    private Storage gcsClient = null;
    private HttpTransport gcsHttpTransport;
    private FileDataStoreFactory gcsDataStoreFactory;
    private final java.io.File GCS_DATA_STORE_DIR =
            new java.io.File(System.getProperty("user.home"), MirrorConstants.GCS_CREDENTIAL_STORAGE_FILE);
    private final JsonFactory GCS_JSON_FACTORY = JacksonFactory.getDefaultInstance();


    public void createClient(MirrorOptions options) throws IllegalStateException, IllegalArgumentException {

        //check Application Name
        if (options.getGCS_APPLICATION_NAME() == null) {
            log.error("Please provide application name for GCS");
            throw new IllegalArgumentException("Please provide application name for GCS");
        }

        try {
            gcsHttpTransport = GoogleNetHttpTransport.newTrustedTransport();
            gcsDataStoreFactory = new FileDataStoreFactory(GCS_DATA_STORE_DIR);

            // authorization, check credentials
            Credential gcsCredential = null;
            try {
                gcsCredential = gcsAuthorize();
            } catch (IllegalArgumentException e){
                log.error("Please provide valid GCS credentials");
                Throwables.propagate(e);
            }catch (Exception e) {
                log.error("Failed to create GCS client. Credentials for GCS maybe invalid: {}", e);
                throw new IllegalStateException("Failed to create GCS client. Credentials for GCS maybe invalid.");
            }
            // set up global Storage instance
            gcsClient = new Storage.Builder(gcsHttpTransport,
                    GCS_JSON_FACTORY,
                    gcsCredential)
                    .setApplicationName(options.getGCS_APPLICATION_NAME())
                    .build();
        } catch (GeneralSecurityException e) {
            log.error("Failed to create GCS client. Client_secret maybe invalid: {}", e);
            throw new IllegalStateException("Failed to create GCS client. Client_secret maybe invalid.");

        } catch (IOException e) {
            log.error("Failed to create GCS client: {}", e);
            throw new IllegalStateException("Failed to create GCS client.");
        }
    }

    @Override
    public Object getClient(MirrorOptions options) {
        try {
            createClient(options);
        }catch (Exception e){
            log.error("Failed to create GCS client: {}", e);
            System.exit(1);
        }
        return this.gcsClient;
    }

    private Credential gcsAuthorize() throws IOException, IllegalArgumentException {

        //load the file
        FileInputStream inputStream = null;
        try {
            File gcs_config = new File("config/gcscfg.json");
            inputStream = new FileInputStream(gcs_config);
        }catch (Exception e){
            log.error("Failed to find the config file for gcs client: ", e);
            throw new IllegalArgumentException("Failed to find the config file for gcs client");
        }

        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(GCS_JSON_FACTORY,
                new InputStreamReader(inputStream));
        if (clientSecrets.getDetails().getClientId().startsWith("Enter") ||
                clientSecrets.getDetails().getClientSecret().startsWith("Enter ")) {
            log.info("Enter CLIENT ID and Secret from https://code.google.com/apis/console/?api=storage_api "
                    + "into config/gcscfg.json");
            throw new IllegalArgumentException("Please provide credentials in config/gcscfg.json");


        }
        // set up authorization code flow
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                gcsHttpTransport, GCS_JSON_FACTORY, clientSecrets,
                Collections.singleton(StorageScopes.DEVSTORAGE_FULL_CONTROL))
                .setDataStoreFactory(gcsDataStoreFactory)
                .build();

        //check id and key are provided
        if (flow.getClientId() == null) {
            log.error("Please provide valid GCS credentials");
            throw new IllegalArgumentException("Please provide valid GCS credentials");
        }

        // gcsAuthorize
        log.warn("GCS opens the browser / provides the url to verify the account. If you find a 401 error returned from the browser, it means that the client_id is invalid. Please abort this application. Provide a valid client_id and try again");
        return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");

    }


}
