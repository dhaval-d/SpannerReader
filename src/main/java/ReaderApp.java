
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


    public static void main(String... args) throws Exception {

        if (args.length != 4) {
            System.err.println("Usage: ReaderApp <instance_id> <database_id> <read_type>");
            return;
        }
        // Instantiates a client
        SpannerOptions options = SpannerOptions.newBuilder()
                                                    .setSessionPoolOption(SessionPoolOptions.newBuilder()
                                                            .setMinSessions(1000)
                                                            .setMaxSessions(1500)
                                                            .setKeepAliveIntervalMinutes(59)
                                                            .setFailIfPoolExhausted()
                                                            .setWriteSessionsFraction(0.8f)
                                                            .build())
                                                    .build();
        Spanner spanner = options.getService();

        // Name of your instance & database.
        String instanceId = args[0];
        String databaseId = args[1];
        String readType = args[2];
        String directoryPath = args[3];

        // calculation parameters
        long totalElapsedTime = 0;
        long totalReadCount = 0;

        //timer
        long startTime = 0L;
        long elapsedTime = 0L;


        try {
            // Creates a database client
            DatabaseClient dbClient = spanner.getDatabaseClient(DatabaseId.of(
                    options.getProjectId(), instanceId, databaseId));

            System.out.println("Started");
            ArrayList<String> keys = readFiles(directoryPath);
            System.out.println("Keys read: " +Integer.toString(keys.size()));

            //loop through all keys
            for(String key:keys){
                ///Based on user selection, perform reads

                if (readType.equals("1")) {   //Stale read
                    startTime = System.currentTimeMillis();
                    performStaleRead(dbClient,key);
                    elapsedTime = System.currentTimeMillis() - startTime;

                } else if (readType.equals("2")) {  //strong read
                    startTime = System.currentTimeMillis();
                    performStrongRead(dbClient,key);
                    elapsedTime = System.currentTimeMillis() - startTime;

                } else if (readType.equals("3")) { //read only transaction
                    startTime = System.currentTimeMillis();
                    performReadOnlyTransaction(dbClient,key);
                    elapsedTime = System.currentTimeMillis() - startTime;

                } else if (readType.equals("4")) { //read-write transaction
                    startTime = System.currentTimeMillis();
                    performReadWriteTransaction(dbClient,key);
                    elapsedTime = System.currentTimeMillis() - startTime;
                }

                totalElapsedTime += elapsedTime;
                totalReadCount += 1;

                if(totalReadCount % 10 == 0 ){
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

        for (int counter=0; counter<2; counter++) {
            String filename = directoryPath +"/"+"file_"+Integer.toString(counter);

            try {
                File file = new File(filename);
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


    public static void performStaleRead(DatabaseClient dbClient,String keyField) throws Exception{
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


    public static void performStrongRead(DatabaseClient dbClient,String keyField)  throws Exception{
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


    public static void performReadOnlyTransaction(DatabaseClient dbClient,String keyField) throws Exception{
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


    public static void performReadWriteTransaction(DatabaseClient dbClient,String keyField) throws Exception{
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
