from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, DoubleType, LongType, StructType, StringType

KAFKA_BROKERS = "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"  # Fixed: use internal broker addresses
SOURCE_TOPIC = 'financial_transactions'
AGGREGATES_TOPIC = 'transaction_aggregates'
ANOMALIES_TOPIC = 'transaction_anomalies'
CHECKPOINT_DIR = '/mnt/spark-checkpoints'
STATES_DIR = '/mnt/spark-state'  # Fixed: added leading slash

spark = (SparkSession.builder
         .appName('FinancialTransactionProcessor')
         # REMOVED: .config('spark.jars.package', ...) - we're using --jars instead
         .config('spark.sql.streaming.checkpointLocation', CHECKPOINT_DIR)
         .config('spark.sql.streaming.stateStore.stateStoreDir', STATES_DIR)
         .config('spark.sql.shuffle.partitions', 20)
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

transaction_schema = StructType([
    StructField('transactionId', StringType(), True),
    StructField('userId', StringType(), True),
    StructField('merchantId', StringType(), True),
    StructField('amount', DoubleType(), True),
    StructField('transactionTime', LongType(), True),
    StructField('transactionType', StringType(), True),
    StructField('location', StringType(), True),
    StructField('paymentMethod', StringType(), True),
    StructField('isInternational', StringType(), True),
    StructField('currency', StringType(), True),
])

kafka_stream = (spark.readStream
                .format('kafka')
                .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                .option("subscribe", SOURCE_TOPIC)
                .option("startingOffsets", "earliest")
                .option("kafka.session.timeout.ms", "60000")
                .option("kafka.request.timeout.ms", "120000")
                .option("kafka.default.api.timeout.ms", "120000")
                .option("kafka.max.poll.interval.ms", "300000")
                .option("failOnDataLoss", "false")
                .load())


transaction_df = kafka_stream.selectExpr("CAST(value as STRING)") \
    .select(from_json(col('value'), transaction_schema).alias("data")) \
    .select("data.*")

transaction_df = transaction_df.withColumn('transactionTimestamp',
                                           (col('transactionTime') / 1000).cast("timestamp"))

aggregated_df = transaction_df.groupBy("merchantId") \
    .agg(
        sum("amount").alias("totalAmount"),
        count("*").alias("transactionCount")
    )

# Fixed: corrected syntax errors
aggregated_query = (aggregated_df
                    .withColumn("key", col("merchantId").cast("string"))
                    .withColumn("value", to_json(struct(
                        col("merchantId"),
                        col("totalAmount"),
                        col("transactionCount")
                    )))
                    .selectExpr("key", "value")
                    .writeStream
                    .format('kafka')
                    .outputMode('update')
                    .option('kafka.bootstrap.servers', KAFKA_BROKERS)
                    .option('topic', AGGREGATES_TOPIC)  # Fixed: removed quotes
                    .option('checkpointLocation', f'{CHECKPOINT_DIR}/aggregates')
                    .start()
                    )

aggregated_query.awaitTermination()