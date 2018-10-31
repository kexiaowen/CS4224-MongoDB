import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;

public class Transaction7 {
    private MongoDatabase mongoDatabase;

    public Transaction7(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }

    public void execute() {
        MongoCollection<Document> customerCollection = mongoDatabase.getCollection("customer");
        MongoCursor<Document> customerIterator = customerCollection.find()
                .sort(Sorts.descending("c_balance")).limit(10).iterator();
        while (customerIterator.hasNext()) {
            Document customer = customerIterator.next();
            String c_first = customer.getString("c_first");
            String c_middle = customer.getString("c_middle");
            String c_last = customer.getString("c_last");
            double c_balance = customer.getDouble("c_balance");
            String w_name = customer.getString("w_name");
            String d_name = customer.getString("d_name");
            System.out.printf("Name of Customer: %s %s %s\n", c_first, c_middle, c_last);
            System.out.printf("Balance of customer's outstanding payment: %f\n", c_balance);
            System.out.printf("Warehouse name of customer: %s\n", w_name);
            System.out.printf("District name of customer: %s\n", d_name);
        }
    }
}
