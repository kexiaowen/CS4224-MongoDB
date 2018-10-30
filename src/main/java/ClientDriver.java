import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Scanner;

import org.bson.Document;

import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class ClientDriver {
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private ReadConcern readConcern;
    private WriteConcern writeConcern;
    private final String DATABASE_NAME = "cs4224";

    public static void main(String[] args) {
        ClientDriver driver = new ClientDriver();
        if (!driver.checkArgumentsAndConfig(args)) { return; }
        String ip = args[0]; // "192.168.48.219";
        int port = 27017;
        driver.connect(ip, port);
        // driver.test();

        long startTime = System.currentTimeMillis();
        int totalXact = driver.readInput();
        long endingTime = System.currentTimeMillis();
        double totalTime = (endingTime - startTime) / 1000.0;
        System.err.printf("\n\n\n");
        System.err.println("Total transaction: " + totalXact);
        System.err.println("Running time: " + totalTime);
        System.err.println("Transaction throughput: " + totalXact / totalTime);
    }

    private int readInput() {
        Scanner sc = new Scanner(System.in);
        int totalXact = 0;
        while (sc.hasNext()) {
            totalXact++;
            String[] firstRow = sc.next().split(",");
            char type = firstRow[0].charAt(0);
            Timestamp time = new Timestamp(System.currentTimeMillis());
            switch (type) {
                // N, P, D, O, S, I, T, or R,
                case 'N':
                    System.out.println("T1: " + time);
                    int CID1 = Integer.valueOf(firstRow[1]);
                    int WID1 = Integer.valueOf(firstRow[2]);
                    int DID1 = Integer.valueOf(firstRow[3]);
                    int numItem = Integer.valueOf(firstRow[4]);
                    int[] item_number = new int[numItem];
                    int[] supplier_warehouse = new int[numItem];
                    int[] quantity = new int[numItem];
                    for (int i = 0; i < numItem; i++) {
                        String[] row = sc.next().split(",");
                        item_number[i] = Integer.valueOf(row[0]);
                        supplier_warehouse[i] = Integer.valueOf(row[1]);
                        quantity[i] = Integer.valueOf(row[2]);
                    }
                    new Transaction1(mongoDatabase, WID1, DID1, CID1, numItem,
                            item_number, supplier_warehouse, quantity).execute();
                    break;
                case 'P':
                    System.out.println("T2: " + time);
                    int WID2 = Integer.valueOf(firstRow[1]);
                    int DID2 = Integer.valueOf(firstRow[2]);
                    int CID2 = Integer.valueOf(firstRow[3]);
                    double payment = Double.valueOf(firstRow[4]);
                    new Transaction2(mongoDatabase, WID2, DID2, CID2, payment).execute();
                    break;
                case 'D':
                    // Transaction 3
                    System.out.println("T3: " + time);
                    int WID3 = Integer.valueOf(firstRow[1]);
                    int CARRIER_ID3 = Integer.valueOf(firstRow[2]);
                    new Transaction3(mongoDatabase, WID3, CARRIER_ID3).execute();
                    break;
                case 'O':
                    System.out.println("T4: " + time);
                    int WID4 = Integer.valueOf(firstRow[1]);
                    int DID4 = Integer.valueOf(firstRow[2]);
                    int CID4 = Integer.valueOf(firstRow[3]);
                    new Transaction4(mongoDatabase, WID4, DID4, CID4).execute();
                    break;
                case 'S':
                    System.out.println("T5: " + time);
                    int WID5 = Integer.valueOf(firstRow[1]);
                    int DID5 = Integer.valueOf(firstRow[2]);
                    double threshold = Double.valueOf(firstRow[3]);
                    int L = Integer.valueOf(firstRow[4]);
                    new Transaction5(mongoDatabase, WID5, DID5, threshold, L).execute();
                    break;
                case 'I':
                    // Transaction 6
                    System.out.println("T6: " + time);
                    int WID6 = Integer.valueOf(firstRow[1]);
                    int DID6 = Integer.valueOf(firstRow[2]);
                    int L6 = Integer.valueOf(firstRow[3]);
                    new Transaction6(mongoDatabase, WID6, DID6, L6).execute();
                    break;
                case 'T':
                    // Transaction 7
                    System.out.println("T7: " + time);
                    new Transaction7(mongoDatabase).execute();
                    break;
                case 'R':
                    System.out.println("T8: " + time);
                    int WID8 = Integer.valueOf(firstRow[1]);
                    int DID8 = Integer.valueOf(firstRow[2]);
                    int CID8 = Integer.valueOf(firstRow[3]);
                    new Transaction8(mongoDatabase, WID8, DID8, CID8).execute();
                    break;
            }
        }
        return totalXact;
    }

    private void test() {
        MongoCollection<Document> collection = mongoDatabase.getCollection("Customer");
        Document myDoc = collection.find().first();
        System.out.println(myDoc.toJson());
    }

    private boolean checkArgumentsAndConfig(String[] args) {
        if (args.length < 3) {
            System.out.println("Wrong argument input, correct format is " +
                    "~/apache-maven-3.5.4/bin/mvn exec:java -Dexec.args=\"[ip_address] [read_concern] [write_concern]\" < [input_file_name]");
            return false;
        }
        String read = args[1];
        String write = args[2];

        if (read.toLowerCase().equals("local") && write.toLowerCase().equals("1")) {
            readConcern = ReadConcern.LOCAL;
            writeConcern = WriteConcern.W1;
        } else if (read.toLowerCase().equals("majority") && write.toLowerCase().equals("majority")) {
            readConcern = ReadConcern.MAJORITY;
            writeConcern = WriteConcern.MAJORITY;
        } else {
            System.out.println("Read concern level can only be local or majority");
            System.out.println("Write concern level can only be 1 or majority");
            return false;
        }
        return true;
    }

    private void connect(String ip, int port) {
        mongoClient = MongoClients.create(
                MongoClientSettings.builder()
                        .applyToClusterSettings(builder ->
                                builder.hosts(Arrays.asList(new ServerAddress(ip, port))))
                        .build());
        mongoDatabase = mongoClient.getDatabase(DATABASE_NAME).withReadConcern(readConcern).withWriteConcern(writeConcern);
    }
}
