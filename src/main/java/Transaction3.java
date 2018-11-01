import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;


public class Transaction3 {
    private MongoDatabase mongoDatabase;
    private int W_ID;
    private int CARRIER_ID;


    public Transaction3(MongoDatabase mongoDatabase, int W_ID, int CARRIER_ID) {
        this.mongoDatabase = mongoDatabase;
        this.W_ID = W_ID;
        this.CARRIER_ID = CARRIER_ID;
    }

    public void execute() {
        MongoCollection<Document> orderCollection = mongoDatabase.getCollection("order");
        MongoCollection<Document> customerCollection = mongoDatabase.getCollection("customer");

        for (int i = 1; i <= 10; i++) {
            Document order = orderCollection.find(and(eq("o_w_id", W_ID), eq("o_d_id", i), eq("o_carrier_id", -1)))
                    .sort(Sorts.ascending("o_id")).first();
            if (order == null) {
                continue;
            }

            // update o_carrier_id
            int o_id = order.getInteger("o_id");

            // increment c_balance and c_delivery_cnt
            int c_id = order.getInteger("o_c_id");
            List<Document> order_lines = (ArrayList<Document>) order.get("order_lines");
            double ol_amount_sum = 0;
            for (Document order_line : order_lines) {
                ol_amount_sum += order_line.getDouble("ol_amount");
            }

            Document customer = customerCollection.find(and(eq("c_w_id", W_ID), eq("c_d_id", i), eq("c_id", c_id))).first();
            Double new_balance = customer.getDouble("c_balance") + ol_amount_sum;
            Integer c_delivery_cnt = customer.getInteger("c_delivery_cnt");

            orderCollection.updateOne(and(eq("o_w_id", W_ID), eq("o_d_id", i), eq("o_id", o_id)),
                    combine(
                            set("o_carrier_id", CARRIER_ID),
                            set("o_delivery_d", new Timestamp(System.currentTimeMillis()).toString())
                    )
            );

            customerCollection.updateOne(and(eq("c_w_id", W_ID), eq("c_d_id", i), eq("c_id", c_id)),
                    combine(set("c_balance", new_balance), set("c_delivery_cnt", c_delivery_cnt +1)));
        }
    }
}
