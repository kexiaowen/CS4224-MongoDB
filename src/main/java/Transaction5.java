import java.util.TreeSet;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

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
        MongoCollection<Document> stockCollection = mongoDatabase.getCollection("item_stock");
        MongoCollection<Document> orderCollection = mongoDatabase.getCollection("order");
        MongoCursor<Document> orderIter = orderCollection.find(and(eq("o_w_id", W_ID), eq("o_d_id", D_ID)))
                .sort(Sorts.descending("o_id")).limit(L).iterator();
        while (orderIter.hasNext()) {
            Document order = orderIter.next();
            Document[] orderLines = (Document[]) order.get("order_lines");
            for (Document orderLine : orderLines) {
                int itemId = orderLine.getInteger("ol_i_id");
                items.add(itemId);
            }
        }

        int result = 0;
        for (int itemId : items) {
            Document item = stockCollection.find(eq("i_id", itemId)).first();
            Document[] stocks = (Document[]) item.get("stocks");
            for (Document stock : stocks) {
                if (stock.getInteger("s_w_id") == W_ID) {
                    if (stock.getDouble("s_quantity") < threshold) {
                        result++;
                    }
                }
            }
        }
        System.out.println("Number of items below threshold: " + result);
    }
}
