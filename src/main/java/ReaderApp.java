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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.io.PrintStream;
import java.io.OutputStream;
import java.net.InetAddress;

import io.opencensus.trace.Tracing;
import io.opencensus.trace.Tracer;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * This is an application driver class that runs tasks depending on input from user.
 */
public class ReaderApp {
    // Name of your instance & database.
    private static String instanceId = "";
    private static String databaseId = "";
    private static int minSessions = 100;
    private static int maxSessions = 100;

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
        // Number of threads
        int max_threads  = Integer.parseInt(args[6]) * 2;

        //Read files to get a list of keys
        ArrayList<String> keys=null;
        try {
            keys = readFiles(directoryPath);
            System.out.println("Keys read: " + Integer.toString(keys.size()));
        } catch(Exception ex) {
            System.out.println("File read error: "+ex.getMessage());
        }

        // Avoid printing spanner warning messages
        System.setErr(new PrintStream(new OutputStream() {
            public void write(int b) {
            }
        }));

        // total duration and count
        long totalElapsedTime = 0;
        long totalReadCount = 0;



        // Create spanner options, connection and service objects
        SpannerUtility utility = SpannerUtility.getInstance(minSessions,maxSessions,instanceId,databaseId);

        // warm-up your sessions by running a loop
        utility.warmupSessions(minSessions);

        // Because most of these threads would wait for IO from Spanner I want to double the number of threads in a pool.
        int threadPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        System.out.println("Number of processors avail : "+Integer.toString(threadPoolSize));

        try {
            //loop through all keys
            for(String key:keys) {
                // get tracer object
                final Tracer tracer = Tracing.getTracer();
                // create a pool with
                ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadPoolSize);

                //execute based on readType selected by user
                switch(readType) {
                    case "1":
                        for (int i = 0; i < max_threads; i++) {
                            StaleRead task = new StaleRead(tracer,key, utility.getDbClient(),i,utility.getHostName());
                            Future<Long> elapsed = executor.submit(task);
                            totalElapsedTime += elapsed.get();
                        }
                        break;
                    case "2":

                        break;
                    case "3":

                        break;
                    case "4":
                        // try-catch needed because I am rolling back txn by throwing exception
                        try{


                        } catch(Exception ex) {
                        }
                        break;
                }
                executor.shutdown();

                // calculate total records
                totalReadCount += max_threads;
                // print feedback every 1000 reads
                if(totalReadCount % 1000 == 0 ){
                    System.out.println("results");
                    printStatus(totalElapsedTime, totalReadCount);
                }
            }
        } finally {
            // Closes the client which will free up the resources used
            utility.closeService();
            // Prints the results
            System.out.println("\n\n FINAL RESULTS");
            printStatus(totalElapsedTime, totalReadCount);
        }
    }



    // This method reads all 1000 files and returns a list of keys into ArrayList
    private static ArrayList<String> readFiles(String directoryPath) {
        ArrayList<String> results=new ArrayList<String>();

        // Files have only a key column stored in every line
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





    // Prints status of the process
    private static void printStatus(long totalElapsedTime, long totalReadCount) {
        System.out.println("Total Elapsed Time     :"+Long.toString(totalElapsedTime));
        System.out.println("Total Read Count       :"+Long.toString(totalReadCount));
        if(totalReadCount!=0)
            System.out.println("Average Read Time/Op   :"+Float.toString((float)totalElapsedTime/((float)totalReadCount)));
    }
}
