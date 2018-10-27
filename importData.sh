#!/usr/bin/env bash

/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongo 192.168.48.219:27017 loadData.js
/temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/mongoimport --host 192.168.48.219 --port 27017 --db test --collection stockMiscell --type json --file ~/jsonProcess/stock_miscellaneous.json
