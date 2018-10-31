import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.mongodb.client.model.Filters.and;

public class FinalState {

    private MongoDatabase mongoDatabase;

    public FinalState(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }

    public void execute() {
        System.out.printf("\n\nFinal State: \n");

        // #1
        Document d1 = mongoDatabase.getCollection("district").aggregate(
                Arrays.asList(Aggregates.group(null, Accumulators.sum("sum_w_ytd", "$w_ytd")))
        ).first();
        double sumWYTD = d1.getDouble("sum_w_ytd");
        System.out.println("Sum of Warehouse YTD: " + sumWYTD);

        //#2
        Document d2 = mongoDatabase.getCollection("next_avail_order").aggregate(
                Arrays.asList(Aggregates.group(null, Accumulators.sum("sum_next_oid", "$d_next_o_id")))
        ).first();
        int sumDNOID = d2.getInteger("sum_next_oid");
        MongoCursor<Document> d2DistrictIter = mongoDatabase.getCollection("district").find().iterator();
        double sumDYTD = 0;
        while (d2DistrictIter.hasNext()) {
            Document warehouse = d2DistrictIter.next();
            List<Document> districts = (ArrayList<Document>) warehouse.get("districts");
            for (Document district : districts) {
                sumDYTD += district.getDouble("d_ytd");
            }
        }
        System.out.printf("Sum of District YTD: %f, Sum of District next O_ID: %d\n",
                sumDYTD, sumDNOID);

        // #3
        Document d3 = mongoDatabase.getCollection("customer").aggregate(
                Arrays.asList(
                        Aggregates.group(null,
                                Accumulators.sum("sum_c_balance", "$c_balance"),
                                Accumulators.sum("sum_c_ytd_payment", "$c_ytd_payment"),
                                Accumulators.sum("sum_payment_cnt", "$c_payment_cnt"),
                                Accumulators.sum("sum_c_delivery_cnt", "$c_delivery_cnt"))
                )
        ).first();
        double sumCBalance = d3.getDouble("sum_c_balance");
        double sumCYTD = d3.getDouble("sum_c_ytd_payment");
        int sumCPaymentCNT = d3.getInteger("sum_payment_cnt");
        int sumCDeliveryCNT = d3.getInteger("sum_c_delivery_cnt");
        System.out.printf("Sum of Balance: %f, Sum of YTD payment: %f, " +
                "Sum of payment CNT: %d, Sum of Delivery CNT: %d\n",
                sumCBalance, sumCYTD, sumCPaymentCNT, sumCDeliveryCNT);

        // #4
        Document d4 = mongoDatabase.getCollection("order").aggregate(
                Arrays.asList(
                        Aggregates.group(null,
                                Accumulators.max("max_o_id", "$o_id"),
                                Accumulators.sum("sum_o_ol_cnt", "$o_ol_cnt"))
                )
        ).first();
        int maxOId = d4.getInteger("max_o_id");
        double sumOOLCNT = d4.getDouble("sum_o_ol_cnt");
        System.out.printf("Max O_ID: %d, Sum Order line count: %f\n", maxOId, sumOOLCNT);

        // #5
        double sumOLAmt = 0, sumOLQNT = 0;
        MongoCursor<Document> d5Iter = mongoDatabase.getCollection("order").find().iterator();
        while (d5Iter.hasNext()) {
            Document order = d5Iter.next();
            List<Document> orderLines = (ArrayList<Document>) order.get("order_lines");
            for (Document orderLine : orderLines) {
                sumOLAmt += orderLine.getDouble("ol_amount");
                sumOLQNT += orderLine.getDouble("ol_quantity");
            }
        }
        System.out.printf("Sum of Order line amount: %f, Sum of Order line quantity: %f\n",
                sumOLAmt, sumOLQNT);

        // #6
        double sumSQNT = 0, sumSYTD = 0;
        int sumSOCNT = 0, sumSRmtCNT = 0;
        for (Document itemStock : mongoDatabase.getCollection("item_stock").find()) {
            List<Document> stocks = (ArrayList<Document>) itemStock.get("stocks");
            for (Document stock : stocks) {
                sumSQNT += stock.getDouble("s_quantity");
                sumSYTD += stock.getDouble("s_ytd");
                sumSOCNT += stock.getInteger("s_order_cnt");
                sumSRmtCNT += stock.getInteger("s_remote_cnt");
            }
        }

        System.out.printf("Sum of stock quantity: %f, Sum of Stock YTD: %f," +
                        " Sum of stock order count: %d, Sum of stock remote order count: %d\n",
                sumSQNT, sumSYTD, sumSOCNT, sumSRmtCNT);
    }
}
