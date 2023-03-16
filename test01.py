from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.fpm import FPGrowth

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Cyber Intrusion Detection") \
    .getOrCreate()

# Read logs from cloud storage
logs_df = spark.read.text("s3://your-cloud-storage-bucket/logs/")

# Define schema and extract fields
schema = "timestamp STRING, ip STRING, event STRING, details STRING"
fields = [col("value").substr(i, j) for i, j in zip([1, 25, 45, 66], [24, 20, 21, 34])]
logs_df = logs_df.selectExpr(*fields).toDF(*schema.split(", "))

# Preprocess and clean data
logs_df = logs_df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
logs_df = logs_df.dropna(subset=["timestamp", "ip", "event"])

# Aggregate logs by IP address and concatenate events
logs_agg_df = logs_df.groupBy("ip").agg(concat_ws(",", collect_list("event")).alias("events"))

# Set the minimum support and confidence
min_support = 0.1
min_confidence = 0.7

# Create a list of events for each IP address
logs_agg_df = logs_agg_df.withColumn("events", split(col("events"), ","))

# Apply the FPGrowth algorithm
fp_growth = FPGrowth(itemsCol="events", minSupport=min_support, minConfidence=min_confidence)
model = fp_growth.fit(logs_agg_df)

# Display frequent itemsets
frequent_itemsets = model.freqItemsets
frequent_itemsets.show()

# Display generated association rules
association_rules = model.associationRules
association_rules.show()

# Set the threshold for the number of rules triggered
rule_trigger_threshold = 3

# Get the intrusion detection results
intrusion_detection = model.transform(logs_agg_df).filter(size(col("prediction")) >= rule_trigger_threshold)
intrusion_detection.show()