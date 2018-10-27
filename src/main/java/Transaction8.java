import java.util.ArrayList;

import com.mongodb.client.MongoDatabase;

public class Transaction8 {
    class Customer {
        public int cwid, cdid, cid;
        public Customer(int cwid, int cdid, int cid) {
            this.cwid = cwid;
            this.cdid = cdid;
            this.cid = cid;
        }

        public String toString() {
            return String.format("%d, %d, %d", cwid, cdid, cid);
        }
    }

    private int W_ID, D_ID, C_ID;
    private MongoDatabase mongoDatabase;
    private ArrayList<Customer> relatedCustomer;
    private ArrayList<ArrayList<Integer>> targetOrderLines;

    public Transaction8(MongoDatabase mongoDatabase, int W_ID, int D_ID, int C_ID) {
        this.mongoDatabase = mongoDatabase;
        this.W_ID = W_ID;
        this.D_ID = D_ID;
        this.C_ID = C_ID;
        relatedCustomer = new ArrayList<>();
        targetOrderLines = new ArrayList<>();
    }

    public void execute() {

    }
}
