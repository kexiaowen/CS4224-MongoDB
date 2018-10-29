import java.sql.Timestamp;
import java.util.ArrayList;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

import javax.print.Doc;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

public class Transaction1 {
    private MongoDatabase mongoDatabase;
    private String W_ID;
    private String D_ID;
    private String C_ID;
    private int num_items;
    private int[] item_number;
    private int[] supplier_warehouse;
    private int[] quantity;
    private String[] itemName;
    private double[] itemPrice;
    private int[] stockQuantity;
    private Timestamp timestamp;
    MongoCollection<Document> warehouseCollection;
    MongoCollection<Document> stockCollection;
    MongoCollection<Document> itemCollection;
    MongoCollection<Document> distInfoCollection;
    MongoCollection<Document> orderCollection;


    public Transaction1(MongoDatabase mongoDatabase, int W_ID, int D_ID, int C_ID, int num_items,
                        int[] item_number, int[] supplier_warehouse, int[] quantity) {
        this.mongoDatabase = mongoDatabase;
        this.W_ID = String.valueOf(W_ID);
        this.D_ID = String.valueOf(D_ID);
        this.C_ID = String.valueOf(C_ID);
        this.num_items = num_items;
        this.item_number = item_number;
        this.supplier_warehouse = supplier_warehouse;
        this.quantity = quantity;
        itemName = new String[num_items];
        itemPrice = new double[num_items];
        stockQuantity = new int[num_items];
        warehouseCollection = mongoDatabase.getCollection("warehouse_district_customer");
        stockCollection = mongoDatabase.getCollection("warehouse_stock");
        itemCollection = mongoDatabase.getCollection("item");
        distInfoCollection = mongoDatabase.getCollection("stockMiscell");
        orderCollection = mongoDatabase.getCollection("order");
        timestamp = new Timestamp(System.currentTimeMillis());
    }

    public void execute() {
        int nextOID = retrieveAndUpdateOID();
        Document newOrder = createNewOrder(nextOID);
        double rawAmount = insertOrderAndComputePrice(newOrder);
        printResult(newOrder, rawAmount);
    }

    private int retrieveAndUpdateOID() {
        Document d1 = warehouseCollection.find(and(eq("w_id", W_ID), eq("districts.d_id", D_ID)))
                .projection(fields(include("districts.next_o_id"))).first();
        ObjectId objectId = d1.getObjectId("_id");
        int nextOid = Integer.parseInt(d1.getString("districts.next_o_id"));
        warehouseCollection.updateOne(eq("_id", objectId), set("districts.next_o_id", nextOid + 1));
        return nextOid;
    }

    private Document createNewOrder(int nextOid) {
        String allLocal = "1";
        for (int i = 0; i < num_items; i++) {
            if (supplier_warehouse[i] != Integer.parseInt(W_ID)) {
                allLocal = "0";
                break;
            }
        }

        Document order = new Document()
                .append("o_id", String.valueOf(nextOid))
                .append("o_w_id", W_ID)
                .append("o_d_id", D_ID)
                .append("o_c_id", C_ID)
                .append("o_carrier_id", "null")
                .append("o_ol_cnt", String.valueOf(num_items))
                .append("o_entry_d", timestamp.toString())
                .append("o_all_local", allLocal);
        return order;
    }

    private double insertOrderAndComputePrice(Document order) {
        double totalAmount = 0;
        ArrayList<Document> orderLines = new ArrayList<Document>();
        for (int i = 0; i < num_items; i++) {
            String itemNumber = String.valueOf(item_number[i]);
            Document stock = stockCollection.find(and(eq("w_id", W_ID), eq("stocks.s_i_id", item_number[i])))
                    .projection(include("stocks")).first();
            int sQuantity = Integer.parseInt(stock.getString("stocks.s_quantity"));
            int adjustedQuantity = sQuantity - quantity[i];
            if (adjustedQuantity < 10) { adjustedQuantity += 100; }
            stockQuantity[i] = adjustedQuantity;
            double sYtd = Double.parseDouble(stock.getString("stocks.s_ytd"));
            int orderCnt = Integer.parseInt(stock.getString("stocks.s_order_cnt"));
            int remoteCnt = Integer.parseInt(stock.getString("stocks.s_remote_cnt"));
            if (supplier_warehouse[i] != Integer.parseInt(W_ID)) { remoteCnt++; }
            ObjectId stockId = stock.getObjectId("_id");
            stockCollection.updateOne(eq("_id", stockId), combine(
                    set("stocks.s_quantity", String.valueOf(adjustedQuantity)),
                    set("stocks.s_ytd", String.valueOf(sYtd + quantity[i])),
                    set("stocks.s_order_cnt", String.valueOf(orderCnt + 1)),
                    set("stocks.s_remote_cnt", String.valueOf(remoteCnt))));

            Document item = itemCollection.find(eq("i_id", itemNumber)).first();
            itemName[i] = item.getString("i_name");
            itemPrice[i] = Double.parseDouble(item.getString("i_price"));
            double itemAmount = quantity[i] * itemPrice[i];
            totalAmount += itemAmount;
            Document allDistrictInfo = distInfoCollection.find(and(eq("s_w_id", W_ID),
                    eq("s_i_id", itemNumber))).first();
            String key = "s_dist_" + String.format("%02d", Integer.parseInt(D_ID));
            String distInfo = allDistrictInfo.getString(key);

            Document orderLine = new Document()
                    .append("ol_o_id", order.getString("o_id"))
                    .append("ol_w_id", W_ID)
                    .append("ol_d_id", D_ID)
                    .append("ol_number", String.valueOf(i))
                    .append("ol_i_id", itemNumber)
                    .append("ol_supply_w_id", String.valueOf(supplier_warehouse[i]))
                    .append("ol_quantity", String.valueOf(quantity[i]))
                    .append("ol_amount", String.valueOf(itemAmount))
                    .append("ol_delivery_d", "null")
                    .append("ol_dist_info", distInfo);
            orderLines.add(orderLine);
        }

        order.append("orderlines", orderLines.toArray());
        orderCollection.insertOne(order);

        return totalAmount;
    }

    private void printResult(int nextOid, double rawAmount) {
        Document customer = warehouseCollection.find(and(
                eq("w_id", W_ID),
                eq("districts.d_id", D_ID),
                eq("districts.customers.c_id", C_ID)))
                .first();
    }
}
