import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

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
        System.out.println("Warehouse number: " + W_ID + " and District number:" + D_ID);
        System.out.println("Number of last orders to be examined: " + L);

        MongoCollection<Document> nextAvailCollection = mongoDatabase.getCollection("next_avail_order");
        MongoCollection<Document> orderCollection = mongoDatabase.getCollection("order");
        MongoCollection<Document> customerCollection = mongoDatabase.getCollection("customer");

        Document next_avail_order = nextAvailCollection.find(and(eq("d_w_id", W_ID), eq("d_id", D_ID))).first();
        int next_o_id = next_avail_order.getInteger("d_next_o_id");

        ArrayList<Integer> popular_item_ids = new ArrayList<>();
        ArrayList<String> popular_item_names = new ArrayList<>();

        for(int i = 1; i <= L; i++) {
            // for each order, extract the order_lines
            int o_id = next_o_id - i;
            Document order = orderCollection.find(and(eq("o_w_id", W_ID), eq("o_d_id", D_ID), eq("o_id", o_id))).first();
            List<Document> order_lines = (ArrayList<Document>) order.get("order_lines");

            // get popular item ID set
            ArrayList<Integer> i_ids = new ArrayList<>();
            ArrayList<String> i_names = new ArrayList<>();
            int max_quantity = 0;
            for(Document order_line : order_lines) {
                int ol_quantity = order_line.getInteger("ol_quantity");
                max_quantity = Math.max(max_quantity, ol_quantity);
            }

            for(Document order_line : order_lines) {
                int ol_quantity = order_line.getInteger("ol_quantity");
                if(ol_quantity == max_quantity) {
                    i_ids.add(order_line.getInteger("ol_i_id"));
                    i_names.add(order_line.getString("i_name"));
                }
            }

            popular_item_ids.addAll(i_ids);
            popular_item_names.addAll(i_names);

            //output information
            String entry_d = order.getString("o_entry_d");
            System.out.println("Order ID: " + o_id + " and entry date and time: " + entry_d);

            int c_id = order.getInteger("o_c_id");

            Document customer = customerCollection.find(and(eq("c_w_id", W_ID), eq("c_d_id", D_ID), eq("c_id", c_id))).first();
            String c_first = customer.getString("c_first");
            String c_middle = customer.getString("c_middle");
            String c_last = customer.getString("c_last");
            System.out.println("customer first name: " + c_first + " second name: " + c_middle + " and third name: " + c_last);

            for(int j = 0; j < i_ids.size(); j++) {
                System.out.println("item_name: " + i_names.get(j) + " and quantity ordered: " + max_quantity);
            }
        }

        ArrayList<Integer> popular_item_count = new ArrayList<>();

        for (int i = 0; i < popular_item_ids.size(); i++) {
            popular_item_count.add(0);
        }

        for (int i = 1; i <= L; i++) {
            int o_id = next_o_id - i;
            Document order = orderCollection.find(and(eq("o_w_id", W_ID), eq("o_d_id", D_ID), eq("o_id", o_id))).first();
            List<Document> order_lines = (ArrayList<Document>) order.get("order_lines");

            for (int j = 0; j < popular_item_ids.size(); j++) {
                for (Document order_line : order_lines) {
                    if (order_line.getInteger("ol_i_id") == popular_item_ids.get(j)) {
                        int count = popular_item_count.get(j);
                        popular_item_count.set(j, count+1);
                    }
                }
            }
        }

        // output popular percentage information
        for (int i = 0; i < popular_item_ids.size(); i++) {
            String name = popular_item_names.get(i);
            double percentage = popular_item_count.get(i) * 100.0 / L;

            System.out.printf("Item_name: &s, percentage of orders that contain this popular item: %f%%. \n", name, percentage);
        }
    }
}
