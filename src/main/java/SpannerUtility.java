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
import io.opencensus.contrib.grpc.metrics.RpcViews;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.trace.Tracing;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/***
 * This is a utility class to encapsulate all utility items including Spanner objects and options
 */
public class SpannerUtility {
    private SpannerOptions options;
    private Spanner spanner;
    private String instanceId;
    private String databaseId;
    private DatabaseClient dbClient;

    // static variable single_instance of type Singleton
    private static SpannerUtility single_instance = null;

    private SpannerUtility(int minSessions, int maxSessions,String instanceId, String databaseId ) throws Exception{
        this.instanceId = instanceId;
        this.databaseId = databaseId;

        // Initialize stackdriver tracing
        StackdriverTraceExporter.createAndRegister(
                StackdriverTraceConfiguration.builder().build());
        Tracing.getExportComponent().getSampledSpanStore().registerSpanNamesForCollection(
                Arrays.asList("spanner_reads"));

        // Then the exporter for Stackdriver monitoring/metrics.
        StackdriverStatsExporter.createAndRegister();
        //RpcViews.registerAllCumulativeViews();
        RpcViews.registerAllGrpcViews();

        // Instantiates a Spanner options based on user input
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
        // Build spanner service
        spanner = options.getService();
        // Build a database client to perform transactions
        dbClient = createDbClient();
    }

    // Close Spanner service to release all resources
    protected void closeService(){
        spanner.close();
    }

    // Create a singleton instance for Utility class for Spanner
    protected static SpannerUtility getInstance(int minSessions, int maxSessions,String instanceId, String databaseId){
        if (single_instance == null){
            try{
                single_instance = new SpannerUtility(minSessions,maxSessions,instanceId,databaseId);
            } catch (Exception ex){
                ex.printStackTrace();
            }
        }
        return single_instance;
    }

    // Return Spanner service
    protected Spanner getService(){return spanner;}

    // Return Spanner options
    protected SpannerOptions getOptions(){return options;}

    // Get hostname of a machine
    protected String getHostName() throws UnknownHostException {
        InetAddress address = InetAddress.getLocalHost();
        return address.getHostName();
    }

    // Return database client
    protected DatabaseClient getDbClient() {
        return dbClient;
    }

    // create database client
    private DatabaseClient createDbClient() {
        DatabaseClient dbClient = spanner.getDatabaseClient(DatabaseId.of(
                options.getProjectId(), instanceId, databaseId));
        //tracer.getCurrentSpan().addAnnotation("Created DbClient");
        return dbClient;
    }

    // this code runs through minimum sessions to warm up session pool
    protected void warmupSessions(int minSessions){
        Statement statement = Statement
                .newBuilder("SELECT 1")
                .build();

        for(int counter =0;counter < minSessions; counter++){
            // Queries the database
            try(ResultSet resultSet = this.dbClient
                    .singleUse(TimestampBound.ofExactStaleness(15, TimeUnit.SECONDS))
                    .executeQuery(statement)){
            } finally {
            }
        }
    }
}
