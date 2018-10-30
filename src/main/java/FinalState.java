import com.mongodb.client.MongoDatabase;
import org.bson.Document;


import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;

public class FinalState {

    private MongoDatabase mongoDatabase;

    public FinalState(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }

    public void execute() {
        System.out.printf("\n\nFinal State: \n");
        List<Document> groupList = new ArrayList<>();
        groupList.add(new Document().append("$sum", "$w_tyd"));
        mongoDatabase.getCollection("district").aggregate(groupList).first();
//        System.out.println("Sum of Warehouse YTD: " + sumWYTD);
//
//        String q2 = "SELECT SUM(D_YTD), SUM(D_NEXT_O_ID) FROM District;";
//        Row row2 = session.execute(q2).one();
//        double sumDYTD = row2.getDecimal("SYSTEM.SUM(D_YTD)").doubleValue();
//        int sumDNOID = row2.getInt("SYSTEM.SUM(D_NEXT_O_ID)");
//        System.out.printf("Sum of District YTD: %f, Sum of District next O_ID: %d\n",
//                sumDYTD, sumDNOID);
//
//        String q3 = "SELECT SUM(C_BALANCE), SUM(C_YTD_PAYMENT), SUM(C_PAYMENT_CNT)," +
//                " SUM(C_DELIVERY_CNT) FROM CUSTOMER;";
//        Row row3 = session.execute(q3).one();
//        double sumCBalance = row3.getDecimal("SYSTEM.SUM(C_BALANCE)").doubleValue();
//        float sumCYTD = row3.getFloat("SYSTEM.SUM(C_YTD_PAYMENT)");
//        int sumCPaymentCNT = row3.getInt("SYSTEM.SUM(C_PAYMENT_CNT)");
//        int sumCDeliveryCNT = row3.getInt("SYSTEM.SUM(C_DELIVERY_CNT)");
//        System.out.printf("Sum of Balance: %f, Sum of YTD payment: %f, " +
//                "Sum of payment CNT: %d, Sum of Delivery CNT: %d\n",
//                sumCBalance, sumCYTD, sumCPaymentCNT, sumCDeliveryCNT);
//
//        String q4 = "SELECT MAX(O_ID), SUM(O_OL_CNT) FROM Orders";
//        Row row4 = session.execute(q4).one();
//        int maxOId = row4.getInt("SYSTEM.MAX(O_ID)");
//        double sumOOLCNT = row4.getDecimal("SYSTEM.SUM(O_OL_CNT)").doubleValue();
//        System.out.printf("Max O_ID: %d, Sum Order line count: %f\n", maxOId, sumOOLCNT);
//
//        double sumOLAmt = 0, sumOLQNT = 0;
//        for (int i = 1; i < 11; i++) {
//            for (int j = 1; j < 11; j++) {
//                String q5 = String.format("SELECT SUM(OL_AMOUNT), SUM(OL_QUANTITY) FROM OrderLine_By_WDID" +
//                        " Where OL_W_ID = %d AND OL_D_ID = %d;", i, j);
//                Row row5 = session.execute(q5).one();
//                sumOLAmt += row5.getDecimal("SYSTEM.SUM(OL_AMOUNT)").doubleValue();
//                sumOLQNT += row5.getDecimal("SYSTEM.SUM(OL_QUANTITY)").doubleValue();
//            }
//        }
//        System.out.printf("Sum of Order line amount: %f, Sum of Order line quantity: %f\n",
//                sumOLAmt, sumOLQNT);
//
//        double sumSQNT = 0, sumSYTD = 0;
//        int sumSOCNT = 0, sumSRmtCNT = 0;
//        for (int i = 1; i < 11; i++) {
//            String q6 = String.format("SELECT SUM(S_QUANTITY), SUM(S_YTD), SUM(S_ORDER_CNT), SUM(S_REMOTE_CNT)" +
//                    " FROM Stock_By_WID Where S_W_ID = %d;", i);
//            Row row6 = session.execute(q6).one();
//            sumSQNT += row6.getDecimal("SYSTEM.SUM(S_QUANTITY)").doubleValue();
//            sumSYTD += row6.getDecimal("SYSTEM.SUM(S_YTD)").doubleValue();
//            sumSOCNT += row6.getInt("SYSTEM.SUM(S_ORDER_CNT)");
//            sumSRmtCNT += row6.getInt("SYSTEM.SUM(S_REMOTE_CNT)");
//        }
//        System.out.printf("Sum of stock quantity: %f, Sum of Stock YTD: %f," +
//                        " Sum of stock order count: %d, Sum of stock remote order count: %d\n",
//                sumSQNT, sumSYTD, sumSOCNT, sumSRmtCNT);
    }
}
