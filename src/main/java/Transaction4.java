import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.include;

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
        MongoCollection<Document> orderCollection = mongoDatabase.getCollection("order");
        MongoCollection<Document> customerCollection = mongoDatabase.getCollection("customer");
        Document order = orderCollection.find(and(eq("o_w_id", C_W_ID),
                eq("o_d_id", C_D_ID),
                eq("o_c_id", C_ID)))
                .sort(Sorts.descending("o_id")).limit(1).first();
        String first = order.getString("c_first");
        String middle = order.getString("c_middle");
        String last = order.getString("c_last");

        Document customer = customerCollection.find(and(eq("c_w_id", C_W_ID), eq("c_d_id", C_D_ID),
                eq("c_id", C_ID))).projection(include("c_balance")).first();
        double balance = customer.getDouble("c_balance");
        System.out.printf("Name: %s %s %s. Balance: %f\n", first, middle, last, balance);

        int oId = order.getInteger("o_id");
        String entryDate = order.getString("o_entry_d");
        int carrierId = order.getInteger("o_carrier_id");
        System.out.printf("O_ID: %d, O_ENTRY_D: %s, O_CARRIER_ID: %d\n", oId, entryDate, carrierId);

        String deliverDate = order.getString("o_deliver_d");
        List<Document> items = (ArrayList<Document>) order.get("order_lines");
        for (Document item: items) {
            int itemId = item.getInteger("ol_i_id");
            int supplyWarehouse = item.getInteger("ol_supply_w_id");
            double quantitiy = item.getDouble("ol_quantity");
            double price = item.getDouble("ol_amount");
            System.out.printf("OL_I_ID: %d, OL_SUPPLY_W_ID: %d, OL_QUANTITY: %f, OL_AMOUNT: %f, OL_DELIVERY_D: %s\n",
                    itemId, supplyWarehouse, quantitiy, price, deliverDate);
        }
    }
}
