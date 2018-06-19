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

import com.sun.tools.corba.se.idl.ExceptionEntry;
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



public class SpannerUtility {
    private SpannerOptions options;
    private Spanner spanner;

    private String instanceId;
    private String databaseId;

    private DatabaseClient dbClient;

    // static variable single_instance of type Singleton
    private static SpannerUtility single_instance = null;


    private SpannerUtility(int minSessions, int maxSessions,String instanceId, String databaseId ) throws Exception{
        // Next up let's  install the exporter for Stackdriver tracing.
        // StackdriverExporter.createAndRegister();

        this.instanceId = instanceId;
        this.databaseId = databaseId;


        StackdriverTraceExporter.createAndRegister(
                StackdriverTraceConfiguration.builder().build());
        Tracing.getExportComponent().getSampledSpanStore().registerSpanNamesForCollection(
                Arrays.asList("spanner_reads"));


        // Then the exporter for Stackdriver monitoring/metrics.
        StackdriverStatsExporter.createAndRegister();
        //RpcViews.registerAllCumulativeViews();
        RpcViews.registerAllGrpcViews();

        // Instantiates a client
        options = SpannerOptions.newBuilder()
                .setSessionPoolOption(SessionPoolOptions.newBuilder()
                        .setMinSessions(minSessions)
                        .setMaxSessions(maxSessions)
                        .setKeepAliveIntervalMinutes(1)
                        .setBlockIfPoolExhausted()
                        .setWriteSessionsFraction(0.8f)
                        .setMaxIdleSessions(maxSessions)
                        .build())
                .build();

        spanner = options.getService();


        dbClient = createDbClient();
    }

    // Close Spanner service to release all resources
    protected void closeService(){
        spanner.close();
    }


    public static SpannerUtility getInstance(int minSessions, int maxSessions,String instanceId, String databaseId){
        if (single_instance == null){
            try{
                single_instance = new SpannerUtility(minSessions,maxSessions,instanceId,databaseId);
            } catch (Exception ex){

            }
        }
        return single_instance;
    }


    public DatabaseClient getDbClient() {
        return dbClient;
    }

    // create database client
    private DatabaseClient createDbClient() {
        DatabaseClient dbClient = spanner.getDatabaseClient(DatabaseId.of(
                options.getProjectId(), instanceId, databaseId));
        //tracer.getCurrentSpan().addAnnotation("Created DbClient");
        return dbClient;
    }




}
