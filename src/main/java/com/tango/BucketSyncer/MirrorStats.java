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

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class MirrorStats {

    @Getter
    private final Thread shutdownHook = new Thread() {
        @Override
        public void run() {
            logStats();
            //also write logStats to report
            generateReport();

        }
    };

    @Setter
    private String source;

    @Setter
    private String destination;

    private static final String BANNER = "\n--------------------------------------------------------------------\n";

    public void generateReport() {
        String content = String.format("%s STATS BEGIN: %s --> %s\n %s STATS END %s",
                                       BANNER, source, destination, toString(), BANNER);
        File report = new File(MirrorConstants.REPORT);
        if (!report.exists()) {
            try {
                report.getParentFile().mkdirs();
                report.createNewFile();
            } catch (IOException e) {
                log.error("Failed to create report file: ", e);
            }
        }
        //append to file
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(report, true)));
            out.println(content);
            out.close();
        } catch (IOException e) {
            log.error("Failed to generate report: ", e);
        }
    }

    public void logStats() {
        String content = String.format("%s STATS BEGIN: %s --> %s\n %s STATS END %s",
                BANNER, source, destination, toString(), BANNER);
        log.info(content);
    }

    private long start = System.currentTimeMillis();
    private Date startTime = new Date();

    public final AtomicLong objectsRead = new AtomicLong(0);
    public final AtomicLong objectsCopied = new AtomicLong(0);
    public final AtomicLong copyErrors = new AtomicLong(0);
    public final AtomicLong objectsDeleted = new AtomicLong(0);
    public final AtomicLong deleteErrors = new AtomicLong(0);

    public final AtomicLong copyCount = new AtomicLong(0);
    public final AtomicLong deleteCount = new AtomicLong(0);
    public final AtomicLong getCount = new AtomicLong(0);
    public final AtomicLong bytesCopied = new AtomicLong(0);

    public static final long HOUR = TimeUnit.HOURS.toMillis(1);
    public static final long MINUTE = TimeUnit.MINUTES.toMillis(1);
    public static final long SECOND = TimeUnit.SECONDS.toMillis(1);

    //add errorKeyList
    public static final List errorKeyList = Collections.synchronizedList(new ArrayList());

    public String toString() {
        final long durationMillis = System.currentTimeMillis() - start;
        final double durationMinutes = durationMillis / 60000.0d;
        final String duration = String.format("%d:%02d:%02d", durationMillis / HOUR, (durationMillis % HOUR) / MINUTE, (durationMillis % MINUTE) / SECOND);
        final double readRate = objectsRead.get() / durationMinutes;
        final double copyRate = objectsCopied.get() / durationMinutes;
        final double deleteRate = objectsDeleted.get() / durationMinutes;
        return "Started at: " + startTime.toString() + "\n"
                + "read: " + objectsRead + "\n"
                + "copied: " + objectsCopied + "\n"
                + "copy errors: " + copyErrors + "\n"
                + "deleted: " + objectsDeleted + "\n"
                + "delete errors: " + deleteErrors + "\n"
                + "duration: " + duration + "\n"
                + "read rate: " + readRate + "/minute\n"
                + "copy rate: " + copyRate + "/minute\n"
                + "delete rate: " + deleteRate + "/minute\n"
                + "bytes copied: " + formatBytes(bytesCopied.get()) + "\n"
                + "GET operations: " + getCount + "\n"
                + "COPY operations: " + copyCount + "\n"
                + "DELETE operations: " + deleteCount + "\n"
                + "Error Key List: " + errorKeyList.toString() + "\n"
                + "Ended at: " + (new Date()).toString() + "\n";
    }

    private String formatBytes(long bytesCopied) {
        if (bytesCopied > MirrorConstants.EB)
            return ((double) bytesCopied) / ((double) MirrorConstants.EB) + " EB (" + bytesCopied + " bytes)";
        if (bytesCopied > MirrorConstants.PB)
            return ((double) bytesCopied) / ((double) MirrorConstants.PB) + " PB (" + bytesCopied + " bytes)";
        if (bytesCopied > MirrorConstants.TB)
            return ((double) bytesCopied) / ((double) MirrorConstants.TB) + " TB (" + bytesCopied + " bytes)";
        if (bytesCopied > MirrorConstants.GB)
            return ((double) bytesCopied) / ((double) MirrorConstants.GB) + " GB (" + bytesCopied + " bytes)";
        if (bytesCopied > MirrorConstants.MB)
            return ((double) bytesCopied) / ((double) MirrorConstants.MB) + " MB (" + bytesCopied + " bytes)";
        if (bytesCopied > MirrorConstants.KB)
            return ((double) bytesCopied) / ((double) MirrorConstants.KB) + " KB (" + bytesCopied + " bytes)";
        return bytesCopied + " bytes";
    }

}
