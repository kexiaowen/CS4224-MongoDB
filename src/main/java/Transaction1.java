import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.push;
import static com.mongodb.client.model.Updates.set;

public class Transaction1 {
    private MongoDatabase mongoDatabase;
    private int W_ID;
    private int D_ID;
    private int C_ID;
    private int num_items;
    private int[] item_number;
    private int[] supplier_warehouse;
    private int[] quantity;

    private double warehouseTax, districtTax, discount;
    private String customerCredit;
    private String[] itemName;
    private double[] itemPrice;
    private double[] stockQuantity;
    private Timestamp timestamp;

    public Transaction1(MongoDatabase mongoDatabase, int W_ID, int D_ID, int C_ID, int num_items,
                        int[] item_number, int[] supplier_warehouse, int[] quantity) {
        this.mongoDatabase = mongoDatabase;
        this.W_ID = W_ID;
        this.D_ID = D_ID;
        this.C_ID = C_ID;
        this.num_items = num_items;
        this.item_number = item_number;
        this.supplier_warehouse = supplier_warehouse;
        this.quantity = quantity;
        itemName = new String[num_items];
        itemPrice = new double[num_items];
        stockQuantity = new double[num_items];
        timestamp = new Timestamp(System.currentTimeMillis());
    }

    public void execute() {
        int nextOID = retrieveAndUpdateOID();
        Document newOrder = createNewOrder(nextOID);
        double rawAmount = insertOrderAndComputePrice(newOrder);
        printResult(newOrder, rawAmount);
    }

    private int retrieveAndUpdateOID() {
        MongoCollection<Document> nextAvailCollection = mongoDatabase.getCollection("next_avail_order");
        Document d1 = nextAvailCollection.find(and(eq("d_w_id", W_ID), eq("d_id", D_ID))).first();
        int nextOid = d1.getInteger("d_next_o_id");
        nextAvailCollection.updateOne(and(eq("d_w_id", W_ID), eq("d_id", D_ID)), set("d_next_o_id", nextOid + 1));
        return nextOid;
    }

    private Document createNewOrder(int nextOid) {
        int allLocal = 1;
        for (int i = 0; i < num_items; i++) {
            if (supplier_warehouse[i] != W_ID) {
                allLocal = 0;
                break;
            }
        }

        MongoCollection<Document> customerCollection = mongoDatabase.getCollection("customer");
        Document d2 = customerCollection.find(and(eq("c_w_id", C_ID), eq("c_d_id", D_ID),
                eq("c_id", C_ID))).first();
        String first = d2.getString("c_first");
        String middle = d2.getString("c_middle");
        String last = d2.getString("c_last");
        warehouseTax = d2.getDouble("w_tax");
        districtTax = d2.getDouble("d_tax");
        customerCredit = d2.getString("c_credit");
        discount = d2.getDouble("c_discount");

        Document order = new Document()
                .append("o_id", nextOid)
                .append("o_w_id", W_ID)
                .append("o_d_id", D_ID)
                .append("o_c_id", C_ID)
                .append("o_carrier_id", -1)
                .append("o_ol_cnt", num_items)
                .append("o_entry_d", timestamp.toString())
                .append("o_all_local", allLocal)
                .append("o_delivery_d", "null")
                .append("c_first", first)
                .append("c_middle", middle)
                .append("c_last", last);
        return order;
    }

    private double insertOrderAndComputePrice(Document order) {
        double totalAmount = 0;
        ArrayList<Document> orderLines = new ArrayList<Document>();
        ArrayList<Document> shortOrderLines = new ArrayList<Document>();
        MongoCollection<Document> stockCollection = mongoDatabase.getCollection("item_stock");
        MongoCollection<Document> allStockInfoCollection = mongoDatabase.getCollection("stock_misc");
        for (int i = 0; i < num_items; i++) {

            Document itemStock = stockCollection.find(eq("i_id", item_number[i])).first();
            Document[] stocks = (Document[]) itemStock.get("stocks");
            Document targetStock = null;
            int id = -1;
            for (int j = 0; j < stocks.length; j++) {
                if (stocks[j].getInteger("s_w_id") == W_ID) {
                    targetStock = stocks[j];
                    id = j;
                    break;
                }
            }
            if (targetStock == null) { continue; }
            double sQuantity = targetStock.getDouble("s_quantity");
            double adjustedQuantity = sQuantity - quantity[i];
            if (adjustedQuantity < 10) { adjustedQuantity += 100; }
            stockQuantity[i] = adjustedQuantity;
            double sYtd = targetStock.getDouble("s_ytd");
            int orderCnt = targetStock.getInteger("s_order_cnt");
            int remoteCnt = targetStock.getInteger("s_remote_cnt");
            if (supplier_warehouse[i] != W_ID) { remoteCnt++; }
            // TODO: update the stock info
            stocks[id].put("s_ytd", sYtd + quantity[i]);
            stocks[id].put("s_order_cnt", orderCnt + 1);
            stocks[id].put("s_remote_cnt", remoteCnt);
            stocks[id].put("s_quantity", adjustedQuantity);
            stockCollection.updateOne(eq("i_id", item_number[i]), set("stocks", Arrays.asList(stocks)));

            itemName[i] = itemStock.getString("i_name");
            itemPrice[i] = itemStock.getDouble("i_price");
            double itemAmount = quantity[i] * itemPrice[i];
            totalAmount += itemAmount;
            Document allDistrictInfo = allStockInfoCollection.find(and(eq("s_w_id", W_ID),
                    eq("s_i_id", item_number[i]))).first();
            String key = "s_dist_" + String.format("%02d", D_ID);
            String distInfo = allDistrictInfo.getString(key);

            Document orderLine = new Document()
                    .append("ol_o_id", order.getString("o_id"))
                    .append("ol_w_id", W_ID)
                    .append("ol_d_id", D_ID)
                    .append("ol_number", i)
                    .append("ol_i_id", item_number[i])
                    .append("ol_supply_w_id", supplier_warehouse[i])
                    .append("ol_quantity", quantity[i])
                    .append("ol_amount", itemAmount)
                    .append("ol_delivery_d", "null")
                    .append("ol_dist_info", distInfo)
                    .append("i_name", itemName[i]);
            Document shortOrderLine = new Document().append("ol_i_id", item_number[i]);

            orderLines.add(orderLine);
            shortOrderLines.add(shortOrderLine);
        }
        MongoCollection<Document> orderCollection = mongoDatabase.getCollection("order");
        order.append("order_lines", orderLines);
        orderCollection.insertOne(order);

        MongoCollection<Document> customerOrderCollection = mongoDatabase.getCollection("customer_order");
        Document shortOrder = new Document()
                .append("o_id", order.getInteger("o_id"))
                .append("order_lines", shortOrderLines);
        customerOrderCollection.updateOne(and(eq("c_w_id", W_ID),
                eq("c_d_id", D_ID),
                eq("c_id", C_ID)),
                push("orders", shortOrder));
        return totalAmount;
    }

    private void printResult(Document order, double rawAmount) {
        String lastName = order.getString("c_last");
        System.out.printf("Customer identifier: %d, %d, %d, lastname: %s, credit: %s, discount: %f\n",
                W_ID, D_ID, C_ID, lastName, customerCredit, discount);

        System.out.println("Warehouse_Tax: " + warehouseTax + " District_Tax: " + districtTax);

        System.out.println("Entry date: " + timestamp.toString() + " O_ID: " + order.getInteger("o_id"));

        double totalAmount = rawAmount * (1 + warehouseTax + districtTax) * (1 - discount);
        System.out.println("Num_items: " + num_items + " Total_amount: " + totalAmount);

        for (int i = 0; i < num_items; i++) {
            System.out.printf("Item number: %d, Item name: %s, Supplier warehouse: %d, Quantity: %d, " +
                            "OL_amount: %f, S_quantity: %f\n", item_number[i], itemName[i], supplier_warehouse[i],
                    quantity[i], itemPrice[i] * quantity[i], stockQuantity[i]);
        }
    }
}
