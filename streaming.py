from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat_ws, explode, current_timestamp, count, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Initialisation de Spark avec les connecteurs nécessaires
spark = SparkSession.builder \
    .appName("KafkaSparkTest") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.apache.spark:spark-sql-connector-cassandra_2.12:3.5.0") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/user_db.raw_users") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

# Schéma JSON complet (inchangé, bien défini dans votre code)
user_schema = StructType([
    StructField("results", ArrayType(
        StructType([
            StructField("gender", StringType(), True),
            StructField("name", StructType([
                StructField("title", StringType(), True),
                StructField("first", StringType(), True),
                StructField("last", StringType(), True)
            ]), True),
            StructField("location", StructType([
                StructField("street", StructType([
                    StructField("number", IntegerType(), True),
                    StructField("name", StringType(), True)
                ]), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("country", StringType(), True),
                StructField("postcode", StringType(), True),
                StructField("coordinates", StructType([
                    StructField("latitude", StringType(), True),
                    StructField("longitude", StringType(), True)
                ]), True),
                StructField("timezone", StructType([
                    StructField("offset", StringType(), True),
                    StructField("description", StringType(), True)
                ]), True)
            ]), True),
            StructField("email", StringType(), True),
            StructField("login", StructType([
                StructField("uuid", StringType(), True),
                StructField("username", StringType(), True),
                StructField("password", StringType(), True),
                StructField("salt", StringType(), True),
                StructField("md5", StringType(), True),
                StructField("sha1", StringType(), True),
                StructField("sha256", StringType(), True)
            ]), True),
            StructField("dob", StructType([
                StructField("date", StringType(), True),
                StructField("age", IntegerType(), True)
            ]), True),
            StructField("registered", StructType([
                StructField("date", StringType(), True),
                StructField("age", IntegerType(), True)
            ]), True),
            StructField("phone", StringType(), True),
            StructField("cell", StringType(), True),
            StructField("id", StructType([
                StructField("name", StringType(), True),
                StructField("value", StringType(), True)
            ]), True),
            StructField("picture", StructType([
                StructField("large", StringType(), True),
                StructField("medium", StringType(), True),
                StructField("thumbnail", StringType(), True)
            ]), True),
            StructField("nat", StringType(), True)
        ])
    ), True)
])

# Lecture depuis Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "userData") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parsing JSON
parsed_df = kafka_df \
    .select(from_json(col("value").cast("string"), user_schema).alias("data")) \
    .select("data.results")

# Explosion du tableau
exploded_df = parsed_df \
    .select(explode(col("results")).alias("user"))

# Enrichissement des données
enriched_df = exploded_df \
    .select(
        col("user.login.uuid").alias("user_id"),  # Clé primaire pour Cassandra
        current_timestamp().alias("ingestion_time"),
        col("user.gender").alias("gender"),
        concat_ws(" ", col("user.name.title"), col("user.name.first"), col("user.name.last")).alias("full_name"),
        col("user.location.country").alias("country"),
        col("user.dob.age").alias("age"),
        col("user.email").alias("email"),
        col("user.phone").alias("phone"),
        col("user.nat").alias("nationality")
    )

# Fonction de traitement par batch
def process_batch(batch_df, batch_id):
    # Écriture dans MongoDB (données brutes enrichies)
    batch_df.write \
        .format("mongodb") \
        .mode("append") \
        .option("uri", "mongodb://localhost:27017") \
        .option("database", "user_db") \
        .option("collection", "raw_users") \
        .save()

    # Agrégations par pays
    country_agg_df = batch_df.groupBy("country").agg(
        count("*").alias("count_users"),
        avg("age").alias("avg_age")
    ).withColumn("last_update", current_timestamp())

    # Écriture des utilisateurs dans Cassandra
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="random_user_table", keyspace="random_user_keyspace") \
        .save()

    # Écriture des agrégations dans Cassandra
    country_agg_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="country_stats", keyspace="random_user_keyspace") \
        .save()

    # Debug console
    batch_df.write.format("console").option("truncate", False).save()
    country_agg_df.write.format("console").option("truncate", False).save()

# Lancement du streaming
query = enriched_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "/tmp/checkpoint/spark_streaming") \
    .start()

# Attente de terminaison
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Arrêt du streaming Spark...")
    spark.stop()
    print("Streaming arrêté.")