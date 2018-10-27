import com.mongodb.client.MongoDatabase;

public class Transaction6 {
    private MongoDatabase mongoDatabase;
    private int W_ID;
    private int D_ID;
    private int L;

    public Transaction6(MongoDatabase mongoDatabase, int W_ID, int D_ID, int L) {
        this.mongoDatabase = mongoDatabase;
        this.W_ID = W_ID;
        this.D_ID = D_ID;
        this.L = L;
    }

    public void execute() {

    }
}
