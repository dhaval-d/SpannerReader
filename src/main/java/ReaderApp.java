
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
import io.opencensus.exporter.trace.stackdriver.StackdriverExporter;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;


import java.util.Arrays;
import java.io.PrintStream;
import java.io.OutputStream;

public class ReaderApp {

    // Name of your instance & database.
    private static String instanceId = "";
    private static String databaseId = "";
    private static String parentSpanName = "read-keys";

    private static int minSessions = 0;
    private static int maxSessions = 0;

    private SpannerOptions options;
    private Spanner spanner;


    public ReaderApp() throws Exception{
        // Next up let's  install the exporter for Stackdriver tracing.
        StackdriverExporter.createAndRegister();
        Tracing.getExportComponent().getSampledSpanStore().registerSpanNamesForCollection(
                Arrays.asList(parentSpanName));


        // Then the exporter for Stackdriver monitoring/metrics.
        StackdriverStatsExporter.createAndRegister();
        RpcViews.registerAllCumulativeViews();

        // Instantiates a client
        options = SpannerOptions.newBuilder()
                .setSessionPoolOption(SessionPoolOptions.newBuilder()
                        .setMinSessions(minSessions)
                        .setMaxSessions(maxSessions)
                        .setKeepAliveIntervalMinutes(1)
                        .setFailIfPoolExhausted()
                        .setWriteSessionsFraction(0.00001f)
                        .build())
                .build();

        spanner = options.getService();
        // Avoid printing spanner warning messages
        System.setErr(new PrintStream(new OutputStream() {
            public void write(int b) {
            }
        }));


    }


    // Next up let's  install the exporter for Stackdriver tracing.
    public static void main(String... args) throws Exception {
        if (args.length != 6) {
            System.err.println("Usage: ReaderApp <instance_id> <database_id> <read_type> <directory_path> <min_sessions> <max_sessions>");
            return;
        }
        // Name of your instance & database.
        instanceId = args[0];
        databaseId = args[1];
        String readType = args[2];
        String directoryPath = args[3];
        minSessions = Integer.parseInt(args[4]);
        maxSessions = Integer.parseInt(args[5]);

        //Read files to get a list of keys
        ArrayList<String> keys=null;
        try {
            keys = readFiles(directoryPath);
            System.out.println("Keys read: " + Integer.toString(keys.size()));
        } catch(Exception ex) {

        }

        // calculation parameters
        long totalElapsedTime = 0;
        long totalReadCount = 0;
        //timer
        long startTime = 0L;
        long elapsedTime = 0L;

        ReaderApp rApp= new ReaderApp();

            String childWorkSpan = "";
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
        try {
            //loop through all keys
            for(String key:keys) {
                ///Based on user selection, perform reads
                try (Scope ss = Tracing.getTracer()
                        .spanBuilder(childWorkSpan)
                        // Enable the trace sampler.
                        //  We are always sampling for demo purposes only: this is a very high sampling
                        // rate, but sufficient for the purpose of this quick demo.
                        // More realistically perhaps tracing 1 in 10,000 might be more useful
                        .setSampler(Samplers.alwaysSample())
                        .startScopedSpan()) {

                    if (readType.equals("1")) {   //Stale read
                        startTime = System.currentTimeMillis();
                        rApp.performStaleRead(key);
                        elapsedTime = System.currentTimeMillis() - startTime;
                    } else if (readType.equals("2")) {  //strong read
                        startTime = System.currentTimeMillis();
                        rApp.performStrongRead(key);
                        elapsedTime = System.currentTimeMillis() - startTime;

                    } else if (readType.equals("3")) { //read only transaction
                        startTime = System.currentTimeMillis();
                        rApp.performReadOnlyTransaction(key);
                        elapsedTime = System.currentTimeMillis() - startTime;

                    } else if (readType.equals("4")) { //read-write transaction
                        startTime = System.currentTimeMillis();
                        try{
                            rApp.performReadWriteTransaction(key);
                        } catch(Exception ex){

                        }

                        elapsedTime = System.currentTimeMillis() - startTime;
                    }
                }
                 finally {
                }

                totalElapsedTime += elapsedTime;
                totalReadCount += 1;

                if(totalReadCount % 100 == 0 ){
                    System.out.println("Total Elapsed Time     :"+Long.toString(totalElapsedTime));
                    System.out.println("Total Read Count       :"+Long.toString(totalReadCount));
                    System.out.println("Average Read Time/Op   :"+Float.toString(totalElapsedTime/totalReadCount));
                }
            }
        } finally {
            // Closes the client which will free up the resources used
            rApp.closeService();

            // Prints the results
            System.out.println("\n\n FINAL RESULTS");
            System.out.println("Total Elapsed Time     :"+Long.toString(totalElapsedTime));
            System.out.println("Total Read Count       :"+Long.toString(totalReadCount));
            System.out.println("Average Read Time/Op   :"+Float.toString(totalElapsedTime/totalReadCount));

        }
    }

    /*
    This method reads all 1000 files and returns a list of keys into ArrayList
     */
    public static ArrayList<String> readFiles(String directoryPath) {
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

    // Close Spanner service to release all resources
    private void closeService(){
            spanner.close();
    }

    // Perform a stale read with exact staleness of 15 seconds
    private void performStaleRead(String keyField) throws Exception{
        // Creates a database client
        DatabaseClient dbClient = spanner.getDatabaseClient(DatabaseId.of(
                options.getProjectId(), instanceId, databaseId));
        Statement statement = Statement
                .newBuilder("SELECT pk_field FROM table1 where pk_field= @KEY_FIELD")
                .bind("KEY_FIELD").to(keyField)
                .build();

        ResultSet resultSet = dbClient
                .singleUse(TimestampBound.ofExactStaleness(15, TimeUnit.SECONDS)).executeQuery(statement);

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

    // Perform a string read
    private void performStrongRead(String keyField)  throws Exception{
        // Creates a database client
        DatabaseClient dbClient = spanner.getDatabaseClient(DatabaseId.of(
                options.getProjectId(), instanceId, databaseId));
        Statement statement = Statement
                .newBuilder("SELECT pk_field FROM table1 where pk_field= @KEY_FIELD")
                .bind("KEY_FIELD").to(keyField)
                .build();

        // Queries the database
        ResultSet resultSet = dbClient
                                .singleUse().executeQuery(statement);

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

    // Perform a readonly transaction
    private void performReadOnlyTransaction(String keyField) throws Exception{
        // Creates a database client
        DatabaseClient dbClient = spanner.getDatabaseClient(DatabaseId.of(
                options.getProjectId(), instanceId, databaseId));
        Statement statement = Statement
                .newBuilder("SELECT pk_field FROM table1 where pk_field= @KEY_FIELD")
                .bind("KEY_FIELD").to(keyField)
                .build();

        // ReadOnlyTransaction must be closed by calling close() on it to release resources held by it.
        // We use a try-with-resource block to automatically do so.
        try (ReadOnlyTransaction transaction = dbClient.readOnlyTransaction()) {
            ResultSet resultSet =
                    transaction.executeQuery(statement);
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

    }

    // Perform a read write transaction and throw an exception to roll back after read
    private void performReadWriteTransaction(String keyField) throws Exception{
        // Creates a database client
        DatabaseClient dbClient = spanner.getDatabaseClient(DatabaseId.of(
                options.getProjectId(), instanceId, databaseId));
        Statement statement = Statement
                .newBuilder("SELECT pk_field FROM table1 where pk_field= @KEY_FIELD")
                .bind("KEY_FIELD").to(keyField)
                .build();

        dbClient
                .readWriteTransaction()
                .run(
                        new TransactionCallable<Void>() {
                            @Override
                            public Void run(TransactionContext transaction) throws Exception {
                                // Transfer marketing budget from one album to another. We do it in a transaction to
                                // ensure that the transfer is atomic.
                                ResultSet resultSet = transaction.executeQuery(statement);
                                while (resultSet.next()) {
                                    String result = resultSet.getString(0);
                                    // match found
                                    if(result.equals(keyField)){
                                        break;
                                    } else {
                                        throw new Exception();
                                    }
                                }

                                throw new Exception();
                            }
                        });
    }

}
