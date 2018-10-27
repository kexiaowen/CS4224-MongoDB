import com.mongodb.client.MongoDatabase;

public class Transaction3 {
    private MongoDatabase mongoDatabase;
    private int W_ID;
    private int CARRIER_ID;
    private int[] O_IDs;
    private int[] C_IDs;

    public Transaction3(MongoDatabase mongoDatabase, int W_ID, int CARRIER_ID) {
        this.mongoDatabase = mongoDatabase;
        this.W_ID = W_ID;
        this.CARRIER_ID = CARRIER_ID;

        // Since there are 10 districts, there is one oldest undelivered order inside every district.
        this.O_IDs = new int[11];
        this.C_IDs = new int[11];
    }

    public void execute() {
    }
}
