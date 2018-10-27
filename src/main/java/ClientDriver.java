import java.util.Arrays;

import org.bson.Document;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class ClientDriver {
    public MongoClient mongoClient;
    public MongoDatabase mongoDatabase;

    public static void main(String[] args) {
        ClientDriver driver = new ClientDriver();
        if (!driver.checkArguments(args)) { return; }
        String ip = args[0]; // "192.168.48.219";
        int port = 27017;
        driver.connect(ip, port);
        driver.test();
    }

    private void test() {
        MongoCollection<Document> collection = mongoDatabase.getCollection("Customer");
        Document myDoc = collection.find().first();
        System.out.println(myDoc.toJson());
    }

    private boolean checkArguments(String[] args) {
        if (args.length < 2) {
            System.out.println("Wrong argument input, correct format is " +
                    "~/apache-maven-3.5.4/bin/mvn exec:java -Dexec.args=\"[ip_address] [consistency_level] < [input_file_name]\"");
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
        mongoDatabase = mongoClient.getDatabase("test");
    }
}
