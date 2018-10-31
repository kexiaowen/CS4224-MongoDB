db = db.getSiblingDB('cs4224')
db.dropDatabase()

db = db.getSiblingDB('cs4224')

sh.enableSharding("cs4224")


db.next_avail_order.createIndex({"d_w_id":1, "d_id":1})
sh.shardCollection("cs4224.next_avail_order", {"d_w_id":1})

db.order.createIndex({"o_w_id":1, "o_d_id":1, "o_id":-1})
db.order.createIndex({"o_w_id":1, "o_d_id":1, "o_c_id":1})
db.order.createIndex({"o_w_id":1, "o_d_id":1, "o_carrier_id":1})
sh.shardCollection("cs4224.order", {"o_w_id":1})

db.item_stock.createIndex({"i_id":1})
sh.shardCollection("cs4224.item_stock", {"i_id":1})

db.customer.createIndex({"c_w_id":1, "c_d_id":1, "c_id":1})
db.customer.createIndex({"c_balance":-1})
sh.shardCollection("cs4224.customer", {"c_w_id":1})

db.district.createIndex({"w_id":1})
sh.shardCollection("cs4224.district", {"w_id":1})

db.district_address.createIndex({"w_id":1, "d_id":1})
sh.shardCollection("cs4224.district_address", {"w_id":1})

db.customer_order.createIndex({"c_w_id":1, "c_d_id":1, "c_id":1})
sh.shardCollection("cs4224.customer_order", {"c_w_id":1})

db.stock_misc.createIndex({"s_w_id":1, "w_i_id":1})
sh.shardCollection("cs4224.stock_misc", {"s_w_id":1})

db.item_misc.createIndex({"i_id":1})
sh.shardCollection("cs4224.item_misc", {"i_id":1})

db.customer_misc.createIndex({"c_w_id":1})
sh.shardCollection("cs4224.customer_misc", {"c_w_id":1})