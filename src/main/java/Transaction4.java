import com.mongodb.client.MongoDatabase;

public class Transaction4 {
    private int C_W_ID, C_D_ID, C_ID;
    private MongoDatabase mongoDatabase;

    public Transaction4(MongoDatabase mongoDatabase, int C_W_ID, int C_D_ID, int C_ID) {
        this.mongoDatabase = mongoDatabase;
        this.C_W_ID = C_W_ID;
        this.C_D_ID = C_D_ID;
        this.C_ID = C_ID;
    }

    public void execute() {
    }
}
