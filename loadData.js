db = db.getSiblingDB('cs4224')
db.dropDatabase()

db = db.getSiblingDB('cs4224')

sh.enableSharding("cs4224")
db.stockMiscell.ensureIndex({s_w_id:"hashed"})
sh.shardCollection("cs4224.stockMiscell",{"s_w_id":"hashed"})

db.item.ensureIndex({i_id:"hashed"})
sh.shardCollection("cs4224.item", {"i_id":"hashed"})

db.order.ensureIndex({o_id:"hashed"})
sh.shardCollection("cs4224.order", {"o_id":"hashed"})

db.customerMiscell.ensureIndex({c_id:"hashed"})
sh.shardCollection("cs4224.customerMiscell", {"c_id":"hashed"})

db.warehouse_district_customer.ensureIndex({w_id:"hashed"})
sh.shardCollection("cs4224.warehouse_district_customer", {"w_id":"hashed"})

db.warehouse_stock.ensureIndex({w_id:"hashed"})
sh.shardCollection("cs4224.warehouse_stock", {"w_id":"hashed"})