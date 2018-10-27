db = db.getSiblingDB('test')
db.stockMiscell.ensureIndex({s_w_id:"hashed"})
sh.shardCollection("test.stockMiscell",{"s_w_id":"hashed"})
