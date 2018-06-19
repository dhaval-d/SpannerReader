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

import java.util.ArrayList;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;


import com.google.cloud.spanner.*;
import io.opencensus.common.Scope;
//import io.opencensus.exporter.trace.stackdriver.StackdriverExporter;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import io.opencensus.trace.Tracer;

import java.io.PrintStream;
import java.io.OutputStream;


import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class ReaderApp {
    // Name of your instance & database.
    private static String instanceId = "";
    private static String databaseId = "";
    private static String parentSpanName = "read-keys";

    private static int minSessions = 0;
    private static int maxSessions = 0;


    public ReaderApp(){

    }

    //  This method reads all 1000 files and returns a list of keys into ArrayList
    private static ArrayList<String> readFiles(String directoryPath) {
        ArrayList<String> results=new ArrayList<String>();

        File files = new File(directoryPath);
        String[] strFiles = files.list();

        for (int counter=0; counter < strFiles.length && counter < 10; counter++) {
            try {
                File file = new File(directoryPath+"/"+ strFiles[counter]);
                FileReader fileReader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    results.add(line);
                }
                fileReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return results;
    }

    // main
    public static void main(String... args) throws Exception {
        if (args.length != 7) {
            System.err.println("Usage: ReaderApp <instance_id> <database_id> <read_type> <directory_path> <min_sessions> <max_sessions> <thread_count>");
            return;
        }
        // Name of your instance & database.
        instanceId = args[0];
        databaseId = args[1];
        // type of read
        String readType = args[2];
        // location of files where your keys exist
        String directoryPath = args[3];
        // PoolSession min and max count
        minSessions = Integer.parseInt(args[4]);
        maxSessions = Integer.parseInt(args[5]);
        int max_threads = Integer.parseInt(args[6]);

        //Read files to get a list of keys
        ArrayList<String> keys=null;
        try {
            keys = readFiles(directoryPath);
            System.out.println("Keys read: " + Integer.toString(keys.size()));
        } catch(Exception ex) {

        }
        // Avoid printing spanner warning messages
        System.setErr(new PrintStream(new OutputStream() {
            public void write(int b) {
            }
        }));


        // total duration and count
        long totalElapsedTime = 0;
        long totalReadCount = 0;

        SpannerUtility utility = SpannerUtility.getInstance(minSessions,maxSessions,instanceId,databaseId);

        //warmupSessions
        System.out.println("Running warm up sessions code");
        warmupSessions(minSessions,utility.getDbClient());
        System.out.println("Ended warm up sessions code");

        int threadPoolSize = Runtime.getRuntime().availableProcessors() + 1;
        System.out.println("Number of processors avail : "+Integer.toString(threadPoolSize));

        String childWorkSpan = getTransactionType(readType);
        try {
           //loop through all keys
            for(String key:keys) {
                ///Based on user selection, perform reads
                final Tracer tracer = Tracing.getTracer();

                //Use the executor created by the newCachedThreadPool() method
                //only when you have a reasonable number of threads
                //or when they have a short duration.

                ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadPoolSize);

                //execute based on readType selected by user
                switch(readType) {
                    case "1":
                        for (int i = 0; i < max_threads; i++) {

                                //singleton dbClient
                                StaleRead task = new StaleRead(tracer,key, utility.getDbClient(),i);
                                //instantiate new dbclient
                               // StaleRead task = new StaleRead(tracer,key,utility.getService(),utility.getOptions(),i);
                                Future<Long> elapsed = executor.submit(task);
                                totalElapsedTime += elapsed.get();
                        }
                        //        executor.wait();
                        executor.shutdown();

                        break;
                    case "2":

                        break;
                    case "3":

                        break;
                    case "4":
                        // try-catch needed because I am rolling back txn by throwing exception
                        try{


                        } catch(Exception ex){
                        }
                        break;
                }

                totalReadCount += 1;

                // print feedback every 1000 reads
                if(totalReadCount % 100 == 0 ){
                    System.out.println("results");
                    printStatus(totalElapsedTime, totalReadCount,max_threads);

                }
            }



        } finally {
            // Closes the client which will free up the resources used
            utility.closeService();

            // Prints the results
            System.out.println("\n\n FINAL RESULTS");
            printStatus(totalElapsedTime, totalReadCount,max_threads);
        }
    }

    // this code runs through minimum sessions to warm up session pool
    private static void warmupSessions(int minSessions, DatabaseClient dbClient){
        Statement statement = Statement
                .newBuilder("SELECT 1")
                .build();

        for(int counter =0;counter < minSessions; counter++){
            // Queries the database
            try(ResultSet resultSet = dbClient
                    .singleUse(TimestampBound.ofExactStaleness(15, TimeUnit.SECONDS))
                    .executeQuery(statement)){
            } finally {
            }
        }
    }

    // type of transaction we are running
    private static String getTransactionType(String readType) {
        String childWorkSpan="";
        switch(readType) {
            case "1":
                childWorkSpan = "Stale_Read";
                break;
            case "2":
                childWorkSpan = "Strong_Read";
                break;
            case "3":
                childWorkSpan = "ReadOnly_Transaction";
                break;
            case "4":
                childWorkSpan = "ReadWrite_Transaction";
                break;
        }
        return childWorkSpan;
    }

    // Prints status of the process
    private static void printStatus(long totalElapsedTime, long totalReadCount,int max_threads) {
        System.out.println("Total Elapsed Time     :"+Long.toString(totalElapsedTime));
        System.out.println("Total Read Count       :"+Long.toString(totalReadCount));
        if(totalReadCount!=0)
            System.out.println("Average Read Time/Op   :"+Float.toString((float)totalElapsedTime/((float)totalReadCount*max_threads)));
    }
}
