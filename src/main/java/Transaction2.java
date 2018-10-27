import com.mongodb.client.MongoDatabase;

public class Transaction2 {
    private int C_W_ID, C_D_ID, C_ID;
    private MongoDatabase mongoDatabase;
    private double payment;

    public Transaction2(MongoDatabase mongoDatabase, int C_W_ID, int C_D_ID, int C_ID,
                        double payment) {
        this.mongoDatabase = mongoDatabase;
        this.C_D_ID = C_D_ID;
        this.C_ID = C_ID;
        this.C_W_ID = C_W_ID;
        this.payment = payment;
    }

    public void execute() {

    }
}
