import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Transaction2 {
    private int C_W_ID, C_D_ID, C_ID;
    private MongoDatabase mongoDatabase;
    private double payment;

    public Transaction2(MongoDatabase mongoDatabase, int C_W_ID, int C_D_ID, int C_ID,
                        double payment) {
        this.mongoDatabase = mongoDatabase;
        this.C_D_ID = C_D_ID;
        this.C_ID = C_ID;
        this.C_W_ID = C_W_ID;
        this.payment = payment;
    }

    public void execute() {
        updateDistrict();
        updateAndPrintCustomerInfo();
        printResults();
    }

    private void updateDistrict() {
        MongoCollection<Document> districtCollection = mongoDatabase.getCollection("district");
        // find w_ytd and increment
        Document districtDocument = districtCollection.find(eq("w_id", C_W_ID)).first();
        double w_ytd = districtDocument.getDouble("w_ytd");
        double updated_w_ytd = w_ytd + payment;

        // find target district index
        List<Document> districts = (ArrayList<Document>) districtDocument.get("districts");
        Document targetDistrict = null;
        int districtIndex = -1;
        for (int i = 0; i < districts.size(); i++) {
            if (districts.get(i).getInteger("d_id") == C_D_ID) {
                targetDistrict = districts.get(i);
                districtIndex = i;
                break;
            }
        }

        // replace target district document in districts array list
        if (districtIndex != -1) {
            double d_ytd = targetDistrict.getDouble("d_ytd");
            double updated_d_ytd = d_ytd + payment;
            districts.get(districtIndex).put("d_ytd", updated_d_ytd);
        }

        // perform updates
        districtCollection.updateOne(eq("w_id", C_W_ID), combine(set("w_ytd", updated_w_ytd), set("districts", districts)));
    }

    private void updateAndPrintCustomerInfo() {
        // update
        MongoCollection<Document> customerCollection = mongoDatabase.getCollection("customer");

        Document customerDocument = customerCollection.find(and(eq("c_w_id", C_W_ID), eq("c_d_id", C_D_ID), eq("c_id", C_ID))).first();
        double c_balance = customerDocument.getDouble("c_balance");
        double c_ytd_payment = customerDocument.getDouble("c_ytd_payment");
        int c_payment_cnt = customerDocument.getInteger("c_payment_cnt");
        double updated_c_balance = c_balance - payment;

        String c_first = customerDocument.getString("c_first");
        String c_middle = customerDocument.getString("c_middle");
        String c_last = customerDocument.getString("c_last");
        String c_street_1 = customerDocument.getString("c_street_1");
        String c_street_2 = customerDocument.getString("c_street_2");
        String c_city = customerDocument.getString("c_city");
        String c_state = customerDocument.getString("c_state");
        String c_zip = customerDocument.getString("c_zip");
        String c_phone = customerDocument.getString("c_phone");
        String c_since = customerDocument.getString("c_since");
        String c_credit = customerDocument.getString("c_credit");
        double c_credit_lim = customerDocument.getDouble("c_credit_lim");
        double c_discount = customerDocument.getDouble("c_discount");

        customerCollection.updateOne(and(eq("c_w_id", C_W_ID), eq("c_d_id", C_D_ID), eq("c_id", C_ID)),
                combine(set("c_balance", updated_c_balance),
                        set("c_ytd_payment", c_ytd_payment + payment),
                        set("c_payment_cnt", c_payment_cnt + 1)));

        // update c_balance in order collection
        MongoCollection<Document> orderCollection = mongoDatabase.getCollection("order");

        orderCollection.updateMany(and(eq("o_w_id", C_W_ID), eq("o_d_id", C_D_ID), eq("o_c_id", C_ID)),
                set("c_balance", updated_c_balance));

        // print
        System.out.printf("Id: %d, %d, %d\nName: %s %s %s\nAddress: %s, %s, %s, %s, %s\n"
                        + "Phone: %s\nSince: %s\nCredit: %s\nCredit limit: %f\nDiscount: %f\nBalance: %f\n",
                C_W_ID, C_D_ID, C_ID, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
                c_phone, c_since, c_credit, c_credit_lim, c_discount, updated_c_balance);
    }

    private void printResults() {
        MongoCollection<Document> districtAddressCollection = mongoDatabase.getCollection("district_address");
        Document districtAddrDocument = districtAddressCollection.find(and(eq("w_id", C_W_ID), eq("d_id", C_D_ID))).first();

        String w_street_1 = districtAddrDocument.getString("w_street_1");
        String w_street_2 = districtAddrDocument.getString("w_street_2");
        String w_city = districtAddrDocument.getString("w_city");
        String w_state = districtAddrDocument.getString("w_state");
        String w_zip = districtAddrDocument.getString("w_zip");

        String d_street_1 = districtAddrDocument.getString("d_street_1");
        String d_street_2 = districtAddrDocument.getString("d_street_2");
        String d_city = districtAddrDocument.getString("d_city");
        String d_state = districtAddrDocument.getString("d_state");
        String d_zip = districtAddrDocument.getString("d_zip");

        System.out.printf("Warehouse address: %s, %s, %s, %s, %s\n",
                w_street_1, w_street_2, w_city, w_state, w_zip);

        System.out.printf("District address: %s, %s, %s, %s, %s\n",
                d_street_1, d_street_2, d_city, d_state, d_zip);

        System.out.println("Payment: " + payment);
    }
}
