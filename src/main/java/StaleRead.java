/*
 * Copyright 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Imports the Google Cloud client library
import com.google.cloud.spanner.*;

import io.opencensus.common.Scope;
import io.opencensus.trace.samplers.Samplers;
import io.opencensus.trace.Tracer;

import java.util.concurrent.Callable;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * This class performs a StaleRead for a single key
 */
public class StaleRead implements Callable<Long>  {
    private Tracer tracer;
    private String keyField;
    private DatabaseClient dbClient;
    private int taskId;
    private String hostName;

    // initialize task
    public StaleRead(Tracer tracer, String keyField, DatabaseClient dbClient, int taskId,String hostName){
        this.tracer = tracer;
        this.keyField = keyField;
        this.dbClient = dbClient;
        this.taskId = taskId;
        this.hostName = hostName;
    }

    @Override
    public Long call() {
        long start_time = System.currentTimeMillis();
        try {
            performStaleRead();
        } catch(Exception ex) {
            System.out.println(ex.getMessage()+" - Inside Call()");
        }
        return System.currentTimeMillis() -start_time;
    }

    // Perform a stale read with exact staleness of 15 seconds
    protected void performStaleRead() throws Exception{
        try (Scope ss = tracer
                .spanBuilder("Stale_read - "+ hostName +" - " + Integer.toString(this.taskId))
                // Enable the trace sampler.
                // We are always sampling for demo purposes only: this is a very high sampling
                // rate, but sufficient for the purpose of this quick demo.
                // More realistically perhaps tracing 1 in 10,000 might be more useful
                .setSampler(Samplers.alwaysSample())
                .startScopedSpan()) {
          // Query the database
            String column = "pk_field";
            Struct row =
                    dbClient.singleUse(TimestampBound.ofExactStaleness(15, TimeUnit.SECONDS))
                            .readRow("table1", Key.of(keyField), Collections.singleton(column));
            String result = row.getString(column);

            // match found
            if(result.equals(keyField)){
            } else {
                throw new Exception("Records did not match");
            }
            tracer.getCurrentSpan().addAnnotation("Read row");
        }
        finally {
        }
    }
}
