import java.util.TreeSet;

import com.mongodb.client.MongoDatabase;

public class Transaction5 {
    private MongoDatabase mongoDatabase;
    private int W_ID, D_ID, L;
    private double threshold;
    private TreeSet<Integer> items;

    public Transaction5(MongoDatabase mongoDatabase, int W_ID, int D_ID, double threshold, int L) {
        this.mongoDatabase = mongoDatabase;
        this.W_ID = W_ID;
        this.D_ID = D_ID;
        this.threshold = threshold;
        this.L = L;
        items = new TreeSet<>();
    }

    public void execute() {
    }
}
