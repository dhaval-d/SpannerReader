
// Imports the Google Cloud client library
import com.google.cloud.spanner.*;

import java.util.concurrent.TimeUnit;

import static com.google.cloud.spanner.TransactionRunner.TransactionCallable;

import java.util.ArrayList;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;

public class ReaderApp {

    // Name of your instance & database.
    static String instanceId = "";
    static String databaseId = "";


    public static void main(String... args) throws Exception {

        if (args.length != 4) {
            System.err.println("Usage: ReaderApp <instance_id> <database_id> <read_type>");
            return;
        }
        // Instantiates a client
        SpannerOptions options = SpannerOptions.newBuilder()
                                                    .setSessionPoolOption(SessionPoolOptions.newBuilder()
                                                            .setMinSessions(15000)
                                                            .setMaxSessions(25000)
                                                            .setKeepAliveIntervalMinutes(59)
                                                            .setFailIfPoolExhausted()
                                                            .setWriteSessionsFraction(0.8f)
                                                            .build())
                                                    .build();
        Spanner spanner = options.getService();

        // Name of your instance & database.
        instanceId = args[0];
        databaseId = args[1];
        String readType = args[2];
        String directoryPath = args[3];

        // calculation parameters
        long totalElapsedTime = 0;
        long totalReadCount = 0;

        //timer
        long startTime = 0L;
        long elapsedTime = 0L;


        try {


            System.out.println("Started");
            ArrayList<String> keys = readFiles(directoryPath);
            System.out.println("Keys read: " +Integer.toString(keys.size()));

            //loop through all keys
            for(String key:keys){
                ///Based on user selection, perform reads

                if (readType.equals("1")) {   //Stale read
                    startTime = System.nanoTime();
                    performStaleRead(spanner,options,key);
                    elapsedTime = System.nanoTime() - startTime;

                } else if (readType.equals("2")) {  //strong read
                    startTime = System.nanoTime();
                    performStrongRead(spanner,options,key);
                    elapsedTime = System.nanoTime() - startTime;

                } else if (readType.equals("3")) { //read only transaction
                    startTime = System.nanoTime();
                    performReadOnlyTransaction(spanner,options,key);
                    elapsedTime = System.nanoTime() - startTime;

                } else if (readType.equals("4")) { //read-write transaction
                    startTime = System.nanoTime();
                    performReadWriteTransaction(spanner,options,key);
                    elapsedTime = System.nanoTime() - startTime;
                }

                totalElapsedTime += elapsedTime;
                totalReadCount += 1;

                if(totalReadCount % 10000 == 0 ){
                    System.out.println("Total Elapsed Time     :"+Long.toString(totalElapsedTime));
                    System.out.println("Total Read Count       :"+Long.toString(totalReadCount));
                    System.out.println("Average Read Time/Op   :"+Float.toString(totalElapsedTime/totalReadCount));
                }
            }

            System.out.println("\n\n FINAL RESULTS");
            System.out.println("Total Elapsed Time     :"+Long.toString(totalElapsedTime));
            System.out.println("Total Read Count       :"+Long.toString(totalReadCount));
            System.out.println("Average Read Time/Op   :"+Float.toString(totalElapsedTime/totalReadCount));


            // Prints the results

        } finally {
            // Closes the client which will free up the resources used
            spanner.close();
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


    public static void performStaleRead(Spanner spanner,SpannerOptions options,String keyField) throws Exception{
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


    public static void performStrongRead(Spanner spanner,SpannerOptions options,String keyField)  throws Exception{
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


    public static void performReadOnlyTransaction(Spanner spanner,SpannerOptions options,String keyField) throws Exception{
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


    public static void performReadWriteTransaction(Spanner spanner,SpannerOptions options,String keyField) throws Exception{
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
