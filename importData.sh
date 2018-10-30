#!/bin/bash
-- Updated, havent tested yet --delete before submit

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongo $1:27017 loadData.js
/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host $1 --port 27017 --db cs4224 --collection customer --type json --file ~/jsonProcess/customer.json

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host $1 --port 27017 --db cs4224 --collection customer_misc --type json --file ~/jsonProcess/customer_misc.json

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host $1 --port 27017 --db cs4224 --collection customer_order --type json --file ~/jsonProcess/customer_order.json

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host $1 --port 27017 --db cs4224 --collection district --type json --file ~/jsonProcess/district.json

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host $1 --port 27017 --db cs4224 --collection district_address --type json --file ~/jsonProcess/district_address.json

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host $1 --port 27017 --db cs4224 --collection item_misc --type json --file ~/jsonProcess/item_misc.json

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host $1 --port 27017 --db cs4224 --collection item_stock --type json --file ~/jsonProcess/item_stock.json

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host $1 --port 27017 --db cs4224 --collection next_avail_order --type json --file ~/jsonProcess/next_avail_order.json

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host $1 --port 27017 --db cs4224 --collection order --type json --file ~/jsonProcess/order.json

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host $1 --port 27017 --db cs4224 --collection stock_misc --type json --file ~/jsonProcess/stock_misc.json