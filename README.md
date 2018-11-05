# CS4224-MongoDB

## Installing and Running MongoDB
1. Install MongoDB on every cluster of machines in the `/temp` folder from binary tarball files. Unarchive the files into folder `/temp/mongodb-linux-x86_64-rhel70-4.0.3`
    
            > cd /temp
            > wget https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-rhel70-4.0.3.tgz
            > tar -xzf mongodb-linux-x86_64-rhel70-4.0.3.tgz

2. Create a new sharded cluster that consists of five mongos, the config server replica set, and a shard replica set.
    1. Create directories for storing data and logs for all five nodes
    
            > cd /temp/mongodb-linux-x86_64-rhel70-4.0.3/bin 
            > mkdir -p data/db
            > mkdir -p data/shard
            > mkdir logs

    2. Create the Config Server Replica Set on nodes 0, 2 and 4. Run the following command on each of the three nodes. Replace <node_ip_address> with the IP address of the node.

            > ./mongod --configsvr --replSet cfgrs --dbpath /temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/data/db --logpath /temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/logs/configsvr.log --bind_ip <node_ip_address> --fork
            
        For example:
        
            > ./mongod --configsvr --replSet cfgrs --dbpath /temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/data/db --logpath /temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/logs/configsvr.log --bind_ip 192.168.48.219 --fork
            
	3. Connect a mongo shell to **ONE** of the config server members. Initiate the replica set from the mongo shell by running the rs.initiate() method.
            
            > cd /temp/mongodb-linux-x86_64-rhel70-4.0.3/bin
            > ./mongo --host <config_server_node_ip_address> --port 27019
            
            In the mongo shell, run the following method, and then type `exit` to exit the shell.
            rs.initiate(
                  {
                    _id: "cfgrs",
                    configsvr: true,
                    members: [
                      { _id : 0, host : "192.168.48.219:27019" },
                      { _id : 1, host : "192.168.48.221:27019" },
                      { _id : 2, host : "192.168.48.223:27019" }
                    ]
                  }
                )
            Note: Replace the ip addresses above with the ip addresses of the config server nodes.
            
    4. Create the Shard Replica Sets. For each of the five nodes, run the following commands.
    
            > cd /temp/mongodb-linux-x86_64-rhel70-4.0.3/bin
            > ./mongod --shardsvr --replSet rs  --dbpath /temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/data/shard --logpath /temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/logs/shardsvr.log --bind_ip <node_ip_addr> --fork

        For example, on node with IP address 192.168.48.219, run the following command:

            > ./mongod --shardsvr --replSet rs  --dbpath /temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/data/shard --logpath /temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/logs/shardsvr.log --bind_ip 192.168.48.219 --fork
            
    5. Connect a mongo shell to **ONE** of the replica set members. From the mongo shell, run the rs.initiate() method to initiate the replica set.
    
            > ./mongo --host <node_ip_addr> --port 27018
            
            In the mongo shell, run the following method, and then type `exit` to exit the shell
            rs.initiate( {
                   _id : "rs",
                   members: [
                      { _id: 0, host: "192.168.48.219:27018" },
                      { _id: 1, host: "192.168.48.220:27018" },
                      { _id: 2, host: "192.168.48.221:27018" },
                      { _id: 3, host: "192.168.48.222:27018" },
                      { _id: 4, host: "192.168.48.223:27018" }
                   ]
                })
             Note: Replace the ip addresses above with the ip addresses of the five nodes.
             
    6. Connect mongos to the Sharded Cluster on each of the five nodes in the cluster. 
            
            > ./mongos --configdb cfgrs/<config_server1_ip>:27019,<config_server2_ip>:27019,<config_server3_ip>:27019 --bind_ip <node_ip_addr> --logpath /temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/logs/mongos.log --fork
	    The three config_server_ip are the ips from the step iii.
    
        For example, on node with IP address 192.168.48.220, run the following command: 
    
            > ./mongos --configdb cfgrs/192.168.48.219:27019,192.168.48.221:27019,192.168.48.223:27019 --bind_ip 192.168.48.220 --logpath /temp/mongodb-linux-x86_64-rhel70-4.0.3/bin/logs/mongos.log --fork

    7. Connect a mongo shell to any mongos. Use the sh.addShard() method to add each shard to the cluster.
    
            > ./mongo --host <node_ip_addr> --port 27017
            
            In the mongo shell, run the following method:
            mongos> sh.addShard( "rs/<node_ip_addr>:27018")
            
        For example: 
        
            mongos> sh.addShard( "rs/192.168.48.219:27018")
            
## Installing Maven for Building Source Code
1. Download Maven using the following command:

            > cd ~
            > wget http://www-us.apache.org/dist/maven/maven-3/3.5.4/binaries/apache-maven-3.5.4-bin.tar.gz

2. Extract Maven distribution archive using the following command:

            > tar -xzf apache-maven-3.5.4-bin.tar.gz

## Running the Application
1. Setting up and Building
    1. Put the code files `CS4224-MongoDB` folder at `~/CS4224-MongoDB`

    2. Run the following commands to build a JAR file from the source code:

            > cd CS4224-MongoDB

            > ~/apache-maven-3.5.4/bin/mvn package

    3. The jar file built is found in `~/CS4224-MongoDB/target`

2. Loading Data into MongoDB
    1. Place project data files at directory `~/4224-project-files` so that data files are found in `~/4224-project-files/data-files` and transaction files are found in `~/4224-project-files/xact-files`.
    
    2. Process the json files using the following command:
    
    		> python processJson.py &
	
    	Note that this will take about 15 minutes to finish. Please continue subsequent work until you see "All finished in xxx seconds".

    3. Execute shell script `importData.sh` by running the following commands:

            > cd CS4224-MongoDB

            > sed -i 's/\r$//' importData.sh

            > chmod +x importData.sh

            > ./importData.sh <node_ip_addr>
            
        For example:
            
            > ./importData.sh 192.168.48.219

3. Running Different Transactions Using the Client Driver

            > cd CS4224-MongoDB
            > ~/apache-maven-3.5.4/bin/mvn exec:java -Dexec.args="[ip_address] [read_concern] [write_concern]" < [input_file_name]

            Note:
            * The read concern can only be local or majority. The write concern can only be 1 or majority.
            * No spacing between `-Dexec.args` and `=`, but the spacing is required between the arguments.
