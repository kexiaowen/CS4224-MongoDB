import com.mongodb.DocumentToDBRefTransformer;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

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
        MongoCollection<Document> nextAvailCollection = mongoDatabase.getCollection("next_avail_order");
        MongoCollection<Document> orderCollection = mongoDatabase.getCollection("order");

        Document next_avail_order = nextAvailCollection.find(and(eq("d_w_id", W_ID), eq("d_id", D_ID))).first();
        int next_o_id = next_avail_order.getInteger("d_next_o_id");

        for(int i = 1; i <= L; i++) {
            // for each order, extract the order_lines
            int o_id = next_o_id - i;
            Document order = orderCollection.find(and(eq("o_w_id", W_ID), eq("o_d_id", D_ID), eq("o_id", o_id))).first();
            List<Document> order_lines = (ArrayList<Document>) order.get("order_lines");

            // get popular item ID set
            ArrayList<Integer> i_ids = new ArrayList<>();
            int max_quantity = 0;
            for(Document order_line : order_lines) {
                int ol_quantity = order_line.getInteger("ol_quantity");
                max_quantity = Math.max(max_quantity, ol_quantity);
            }

            for(Document order_line : order_lines) {
                int ol_quantity = order_line.getInteger("ol_quantity");
                if(ol_quantity == max_quantity) {
                    i_ids.add(order_line.getInteger("ol_i_id"));
                }
            }

            //output information 
        }
    }
}
