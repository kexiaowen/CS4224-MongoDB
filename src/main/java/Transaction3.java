import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;


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
        MongoCollection<Document> orderCollection = mongoDatabase.getCollection("order");
        MongoCollection<Document> customerCollection = mongoDatabase.getCollection("customer");

        for (int i = 1; i <= 10; i++) {
            Document order = orderCollection.find(and(eq("o_w_id", W_ID), eq("o_d_id", i), eq("o_carrier_id", -1)))
                    .sort(Sorts.ascending("o_id")).first();

            // update o_carrier_id
            int o_id = order.getInteger("o_id");
            orderCollection.updateOne(and(eq("o_w_id", W_ID), eq("o_d_id", i), eq("o_id", o_id)), set("o_carrier_id", CARRIER_ID));

            // update order o_delivery_d
            orderCollection.updateOne(and(eq("o_w_id", W_ID), eq("o_d_id", i), eq("o_id", o_id)), set("o_delivery_d", new Timestamp(System.currentTimeMillis()).toString()));

            // increment c_balance and c_delivery_cnt
            int c_id = order.getInteger("o_c_id");
            List<Document> order_lines = (ArrayList<Document>) order.get("order_lines");
            double ol_amount_sum = 0;
            for (Document order_line : order_lines) {
                ol_amount_sum += order_line.getDouble("ol_amount");
            }

            Document customer = customerCollection.find(and(eq("c_w_id", W_ID), eq("c_d_id", i), eq("c_id", c_id))).first();

            customerCollection.updateOne(and(eq("c_w_id", W_ID), eq("c_d_id", i), eq("c_id", c_id)), set("c_balance", customer.getDouble("c_balance")+ol_amount_sum));
            customerCollection.updateOne(and(eq("c_w_id", W_ID), eq("c_d_id", i), eq("c_id", c_id)), set("c_delivery_cnt", customer.getInteger("c_delivery_cnt")+1));
        }
    }
}
