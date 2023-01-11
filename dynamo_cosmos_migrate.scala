// SPAWN A DATABRICKS CLUSTER
// TESTED WITH 10.4 LTS (includes Apache Spark 3.2.1, Scala 2.12)

// SET ENVIRONMENT VARIABLES IN YOUR CLUSTER
// AWS_REGION=YOUR_AWS_REGION
// AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
// AWS_SECRET_KEY=YOUR_SECRET_KEY

// INSTALL LIBRARIES (MAVEN COORDINATES) IN YOUR CLUSTER
// com.audienceproject:spark-dynamodb_2.12:1.1.2
// com.azure.cosmos.spark:azure-cosmos-spark_3-2_2-12:4.15.0

// Databricks notebook source
import com.audienceproject.spark.dynamodb.implicits._
import org.apache.spark.sql.SparkSession

// COMMAND ----------

val spark = SparkSession.builder().getOrCreate()
val dynamoDf = spark.read.dynamodb("student")

print(dynamoDf)
dynamoDf.show(100)

// COMMAND ----------

val cosmosEndpoint = "YOUR_COSMOS_ENDPOINT_URL"
val cosmosMasterKey = "YOUR_MASTER_KEY"

// COMMAND ----------

spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cosmosEndpoint)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cosmosMasterKey)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.views.repositoryPath", "/viewDefinitions")

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS cosmosCatalog.SampleDatabase;
// MAGIC CREATE TABLE IF NOT EXISTS cosmosCatalog.SampleDatabase.student
// MAGIC USING cosmos.oltp
// MAGIC TBLPROPERTIES(partitionKeyPath = '/user_id', autoScaleMaxThroughput = '10000', indexingPolicy = 'OnlySystemProperties');
// MAGIC 
// MAGIC -- CREATE TABLE IF NOT EXISTS throughputControlCatalog.SampleDatabase.ThroughputControlStudent
// MAGIC -- USING cosmos.oltp
// MAGIC -- OPTIONS(spark.cosmos.database = 'SampleDatabase')
// MAGIC -- TBLPROPERTIES(partitionKeyPath = '/groupId', autoScaleMaxThroughput = '4000', indexingPolicy = 'AllProperties', defaultTtlInSeconds = '-1');

// COMMAND ----------

import java.util.UUID
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.Instant;

// Adding an id column with unique values
val uuidUdf=udf[String](() => UUID.randomUUID().toString)
val dynamoDfWId = dynamoDf.withColumn("id", uuidUdf())

val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC));

println(s"Starting preparation: ${formatter.format(Instant.now)}")

val writeCfg = Map(
  "spark.cosmos.accountEndpoint" -> cosmosEndpoint,
  "spark.cosmos.accountKey" -> cosmosMasterKey,
  "spark.cosmos.database" -> "SampleDatabase",
  "spark.cosmos.container" -> "student",
  "spark.cosmos.write.strategy" -> "ItemOverwrite",
  "spark.cosmos.write.bulk.enabled" -> "true",
  "spark.cosmos.throughputControl.enabled" -> "true",
//   "spark.cosmos.throughputControl.accountEndpoint" -> throughputControlEndpoint, // Only need if throughput control is configured with different database account
//   "spark.cosmos.throughputControl.accountKey" -> throughputControlMasterKey, // Only need if throughput control is configured with different database account
  "spark.cosmos.throughputControl.name" -> "NYCGreenTaxiDataIngestion",
  "spark.cosmos.throughputControl.targetThroughputThreshold" -> "0.95",
  "spark.cosmos.throughputControl.globalControl.database" -> "SampleDatabase",
  "spark.cosmos.throughputControl.globalControl.container" -> "ThroughputControl",
)

// val df_NYCGreenTaxi_Input = spark.sql("SELECT * FROM dynamoDf")

dynamoDfWId
  .write
  .format("cosmos.oltp")
  .mode("Append")
  .options(writeCfg)
  .save()

println(s"Finished ingestion: ${formatter.format(Instant.now)}")
