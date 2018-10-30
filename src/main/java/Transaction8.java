import java.util.ArrayList;
import java.util.List;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.ne;

public class Transaction8 {
    class Customer {
        public int cwid, cdid, cid;
        public Customer(int cwid, int cdid, int cid) {
            this.cwid = cwid;
            this.cdid = cdid;
            this.cid = cid;
        }

        public String toString() {
            return String.format("C_W_ID: %d, C_D_ID: %d, C_ID: %d", cwid, cdid, cid);
        }
    }

    private int W_ID, D_ID, C_ID;
    private ArrayList<Customer> relatedCustomer;
    private ArrayList<ArrayList<Integer>> targetOrderLines;
    private MongoCollection<Document> customerOrderCollection;

    public Transaction8(MongoDatabase mongoDatabase, int W_ID, int D_ID, int C_ID) {
        this.W_ID = W_ID;
        this.D_ID = D_ID;
        this.C_ID = C_ID;
        relatedCustomer = new ArrayList<Customer>();
        targetOrderLines = new ArrayList<ArrayList<Integer>>();
        customerOrderCollection = mongoDatabase.getCollection("customer_order");
    }

    private void findTargetOrderLines() {
        Document customer = customerOrderCollection.find(and(eq("c_w_id", W_ID),
                eq("c_d_id", D_ID), eq("c_id", C_ID))).first();
        List<Document> orders = (ArrayList<Document>) customer.get("orders");
        for (Document order : orders) {
            ArrayList<Integer> orderLineItem = new ArrayList<Integer>();
            List<Document> orderLines = (ArrayList<Document>) order.get("order_lines");
            for (Document orderLine : orderLines) {
                orderLineItem.add(orderLine.getInteger("ol_i_id"));
            }
            targetOrderLines.add(orderLineItem);
        }
    }

    private boolean hasTwoSameOrderLine(ArrayList<Integer> OL1, ArrayList<Integer> OL2) {
        int counter = 0;
        for (int i = 0; i < OL1.size(); i++) {
            int iid1 = OL1.get(i);
            for (int j = 0; j < OL2.size(); j++) {
                int iid2 = OL2.get(j);
                if (iid1 == iid2) {
                    counter++;
                    if (counter >= 2)
                        return true;
                    else
                        break;
                }
            }
        }
        return false;
    }

    private boolean isRelatedOrder(Document order) {
        List<Document> orderLines = (ArrayList<Document>) order.get("order_lines");
        ArrayList<Integer> itemNumbers = new ArrayList<Integer>();
        for (Document orderLine : orderLines) {
            itemNumbers.add(orderLine.getInteger("ol_i_id"));
        }
        for (ArrayList<Integer> targetOrder : targetOrderLines) {
            if (hasTwoSameOrderLine(targetOrder, itemNumbers)) {
                return true;
            }
        }
        return false;
    }

    private void recordCustomer(Document customer) {
        int wid = customer.getInteger("c_w_id");
        int did = customer.getInteger("c_d_id");
        int cid = customer.getInteger("c_id");
        relatedCustomer.add(new Customer(wid, did, cid));
    }

    public void execute() {
        findTargetOrderLines();
        MongoCursor<Document> customerIter = customerOrderCollection.find(ne("c_w_id", W_ID)).iterator();
        while (customerIter.hasNext()) {
            Document customer = customerIter.next();
            List<Document> orders = (ArrayList<Document>) customer.get("orders");
            for (Document order : orders) {
                if (isRelatedOrder(order)) {
                    recordCustomer(customer);
                    break;
                }
            }
        }
        for (Customer c : relatedCustomer) {
            System.out.println(c);
        }
    }
}
