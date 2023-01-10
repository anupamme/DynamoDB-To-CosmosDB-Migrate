# Migrating Very Large Scale data from Dynamo DB to Cosmos DB

**Problem Definition**: There are tables in Dynamo DB where each table contains 100s of TB of data. The goal is migrate this data into Cosmos DB, with the constraint that the DB is active so there is data coming in as you do the migration.

**Solution Approach**: There are two steps of migration: offline (snapshot) and online (reading the Change Data Capture).

**Offline Migration** Use Spark connectors for [Dynamo DB](https://github.com/audienceproject/spark-dynamodb) and [Cosmos DB](https://github.com/Azure/azure-sdk-for-java/tree/main/sdk/cosmos/azure-cosmos-spark_3_2-12) to connect to each databases. Once we can connect to both databases we get spark dataframes from the source which we then write to the destination db. This script can be run on azure databricks environment and it will scale to TBs of data automatically.

**Online Migration** We use the Kafka connectors to connect to [Dynamo DB Change Stream](https://github.com/trustpilot/kafka-connect-dynamodb) (source) and another connector to connect to [Azure Cosmos DB](https://github.com/microsoft/kafka-connect-cosmosdb) (sink) [4].  There are some constraints in this setting:
- The kafka connector which reads from Dynamo DB CDC is a single threaded connector so it can read upto 2000 events per second. If we are reading with the flag --from-beginning and the table has TBs of data then the connector will take long time (in days) to complete initial sync. So we want to skip initial sync by not setting this flag.
- How do you partition the data in kafka cluster: The ordering within a partition is guaranteed but not across the partitions. Also, if there are trillion records we cannot just use the primary key as partition key because a trillion partitions may not scale well in kafka cluster.
- The sink connector has bulk mode and it should be write in multi-threaded manner so take benefit of partitioning.

**Coordinating Offline and Online Migration**
- When to start source connector: Just the moment before offline migration begins.
- When to start sink connector: Just the moment after the offline migration finishes
