/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongo 192.168.48.219:27017 loadData.js
/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host 192.168.48.219 --port 27017 --db cs4224 --collection stockMiscell --type json --file ~/jsonProcess/stock_miscellaneous.json

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host 192.168.48.219 --port 27017 --db cs4224 --collection item --type json --file ~/jsonProcess/item.json

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host 192.168.48.219 --port 27017 --db cs4224 --collection order --type json --file ~/jsonProcess/order.json

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host 192.168.48.219 --port 27017 --db cs4224 --collection warehouse_district_customer --type json --file ~/jsonProcess/warehouse_district_customer.json

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host 192.168.48.219 --port 27017 --db cs4224 --collection warehouse_stock --type json --file ~/jsonProcess/warehouse_stock.json

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host 192.168.48.219 --port 27017 --db cs4224 --collection customerMiscell --type json --file ~/jsonProcess/customer_miscellaneous.json