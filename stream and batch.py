from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.streaming import DataStreamReader

# Function to preprocess the data
def preprocess_data(df):
    # Filter out network sniffing and unsecured credential-related attack types
    sniffing_unsecured_attacks = ["Network Sniffing", "Unsecured Credential", "Credential Leakage"]
    df = df.filter(col("attack_type").isin(sniffing_unsecured_attacks))

    df = df.select(
        concat_ws("_", "src_ip", "src_port").alias("src"),
        concat_ws("_", "dest_ip", "dest_port").alias("dest"),
        "protocol",
        "request",
        "attack_type",
    )

    df = df.groupBy("attack_type") \
        .agg(
            collect_set("src").alias("src_set"),
            collect_set("dest").alias("dest_set"),
            collect_set("protocol").alias("protocol_set"),
            collect_set("request").alias("request_set"),
        )

    df = df.withColumn("items", array_union("src_set", "dest_set", "protocol_set", "request_set")) \
        .select("attack_type", "items")

    return df

# Function to perform association rule mining
def perform_association_rule_mining(df):
    min_support = 0.1
    min_confidence = 0.6

    fp_growth = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence)
    model = fp_growth.fit(df)

    # Display frequent itemsets and association rules
    frequent_itemsets = model.freqItemsets
    association_rules = model.associationRules

    return frequent_itemsets, association_rules

# Create Spark session
spark = SparkSession.builder \
    .appName("Real-time Intrusion Detection with Batch Rechecking") \
    .master("local[*]") \
    .getOrCreate()

# Define the schema for the input data
input_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("src_ip", StringType(), True),
    StructField("dest_ip", StringType(), True),
    StructField("src_port", IntegerType(), True),
    StructField("dest_port", IntegerType(), True),
    StructField("protocol", StringType(), True),
    StructField("request", StringType(), True),
    StructField("attack_type", StringType(), True),
])

# Read the batch log data
batch_log_data = spark.read.csv("path/to/log_data.csv", header=True, schema=input_schema)

# Preprocess the batch log data
preprocessed_batch_data = preprocess_data(batch_log_data)

# Perform association rule mining on the batch log data
frequent_itemsets, association_rules = perform_association_rule_mining(preprocessed_batch_data)

# Kafka streaming source
kafka_source = "kafka.bootstrap.servers"  # Replace with your Kafka server's address
kafka_topic = "your-kafka-topic"  # Replace with your Kafka topic

# Read streaming data from Kafka
streaming_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_source) \
    .option("subscribe", kafka_topic) \
    .load()

# Deserialize JSON value from Kafka messages
json_schema = input_schema
streaming_data = streaming_data.select(from_json(col("value").cast("string"), json_schema).alias("log")) \
    .select("log.*")

# Preprocess the streaming data
preprocessed_streaming_data = preprocess_data(streaming_data)

# Define a function to update the global state with new data
def update_global_state(new_data, batch_id):
    global frequent_itemsets, association_rules

    # Perform association rule mining on the new data
    new_frequent_itemsets, new_association_rules = perform_association_rule_mining(new_data)

    # Update the global state with the new data
    frequent_itemsets = frequent_itemsets.union(new_frequent_itemsets)
    association_rules = association_rules.union(new_association_rules)

# Start the streaming query with the update_global_state function as a foreachBatch output sink
streaming_query = preprocessed_streaming_data.writeStream \
    .foreachBatch(update_global_state) \
    .outputMode("update") \
    .trigger(processingTime="1 minute") \
    .start()

# Wait for the streaming query to finish
streaming_query.awaitTermination()
