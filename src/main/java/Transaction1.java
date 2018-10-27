import java.sql.Timestamp;

import com.mongodb.client.MongoDatabase;

public class Transaction1 {
    private MongoDatabase mongoDatabase;
    private int W_ID;
    private int D_ID;
    private int C_ID;
    private int num_items;
    private int[] item_number;
    private int[] supplier_warehouse;
    private int[] quantity;
    private String[] itemName;
    private double[] itemPrice;
    private int[] stockQuantity;
    private Timestamp timestamp;

    public Transaction1(MongoDatabase mongoDatabase, int W_ID, int D_ID, int C_ID, int num_items,
                        int[] item_number, int[] supplier_warehouse, int[] quantity) {
        this.mongoDatabase = mongoDatabase;
        this.W_ID = W_ID;
        this.D_ID = D_ID;
        this.C_ID = C_ID;
        this.num_items = num_items;
        this.item_number = item_number;
        this.supplier_warehouse = supplier_warehouse;
        this.quantity = quantity;
        itemName = new String[num_items];
        itemPrice = new double[num_items];
        stockQuantity = new int[num_items];
    }

    public void execute() {
        int nextOID = retrieveAndUpdateOID();
        insertNewOrder(nextOID);
        double rawAmount = insertOrderLinesAndComputePrice(nextOID);
        printResult(nextOID, rawAmount);
    }

    private int retrieveAndUpdateOID() {
        return 0;
    }

    private void insertNewOrder(int nextOid) {}

    private double insertOrderLinesAndComputePrice(int nextOid) {
        return 0;
    }

    private void printResult(int nextOid, double rawAmount) {}
}
