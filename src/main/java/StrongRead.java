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

import java.util.concurrent.TimeUnit;

import static com.google.cloud.spanner.TransactionRunner.TransactionCallable;

import java.util.ArrayList;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;


import io.opencensus.common.Scope;
import io.opencensus.contrib.grpc.metrics.RpcViews;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
//import io.opencensus.exporter.trace.stackdriver.StackdriverExporter;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import io.opencensus.trace.Tracer;

import java.util.Arrays;
import java.io.PrintStream;
import java.io.OutputStream;



public class StrongRead implements Runnable {
    Tracer tracer;
    String keyField;
    DatabaseClient dbClient;

    public StrongRead(Tracer tracer, String keyField, DatabaseClient dbClient){
        this.tracer = tracer;
        this.keyField = keyField;
        this.dbClient = dbClient;
    }

    @Override
    public void run() {
        try {
            performStrongRead();
        } catch(Exception ex) {
        }
    }

    // Perform a string read
    protected void performStrongRead()  throws Exception{
        // Creates a database client
         Statement statement = getQueryStatement(keyField);

        // Queries the database
        try(ResultSet resultSet = dbClient.singleUse().executeQuery(statement)){
            tracer.getCurrentSpan().addAnnotation("Executed Query");
            processResults(keyField, resultSet);
        } finally {
            tracer.getCurrentSpan().addAnnotation("Closed Results");
        }
    }

    // Open resultSet and confirm a match with key else throw an exception
    private void processResults(String keyField, ResultSet resultSet) throws Exception {
        while (resultSet.next()) {
            String result = resultSet.getString(0);
            // match found
            if(result.equals(keyField)){
                break;
            } else {
                throw new Exception();
            }
        }
    }

    // Build Query for Spanner
    protected Statement getQueryStatement(String keyField) {
        Statement statement = Statement
                .newBuilder("SELECT pk_field FROM table1 where pk_field= @KEY_FIELD")
                .bind("KEY_FIELD").to(keyField)
                .build();
        // tracer.getCurrentSpan().addAnnotation("Created Statement");
        return statement;
    }
}



