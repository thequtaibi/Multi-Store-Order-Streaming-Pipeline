from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, timestamp_seconds
import logging

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s: %(message)s"
        )


spark = (
    SparkSession.builder
    .appName("EcommerceStreaming")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,"
        "org.postgresql:postgresql:42.7.8"
    )
    .getOrCreate()
)


class Bronze:
    def __init__(self):
        self.checpoint_dir = "/home/kind/Desktop/project_x/spark streaming/checkpoint"
        self.postgres_config = {
            "url": (
                "jdbc:postgresql://localhost:5432/"
                "ecommerce_practice?stringtype=unspecified"
            ),
            "dbtable": "orders",
            "user": "khlk4004",
            "password": "4004",
            "driver": "org.postgresql.Driver"
        }


    def extract(self):
        return (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                "pkc-30550.qatarcentral.azure.confluent.cloud:9092"
            )
            .option("subscribe", "orders")
            .option("startingOffsets", "earliest")
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option(
                "kafka.sasl.jaas.config",
                'org.apache.kafka.common.security.plain.PlainLoginModule required '
                f'username="XGHEHXEMAZQYC2KF" '
                f'password="cfltHgWM/vc/nHn2OjiklhgXuIUdyYm8iF1sUbqjroN5K3ZAz1z/myO44aRgQH2A";'
            )
            .load()
        )
    def get_schema(self):
        return """
            order_id STRING,
            customer_id STRING,
            store_id STRING,
            orders_itemas ARRAY<
                STRUCT<
                    product_id: STRING,
                    product_name: STRING,
                    product_category: STRING,
                    price: DOUBLE,
                    quantity: INT,
                    approval: BOOLEAN
                >
            >,
            timestamp DOUBLE
        """

    def transform(self, kafka_df):
        parsed_df = kafka_df.select(
            from_json(
                col("value").cast("string"),
                self.get_schema()
            ).alias("data")
        ).select("data.*")

        return parsed_df.select(
            col("order_id"),
            col("customer_id"),
            col("store_id"),
            to_json(col("orders_itemas")).alias("orders_itemas"),
            timestamp_seconds(col("timestamp")).alias("order_timestamp")
        )



    def write_to_postgres(self, batch_df, batch_id):

        print(f"Processing batch: {batch_id}")

        duplicates = (
            batch_df
            .groupBy("order_id")
            .count()
            .filter(col("count") > 1)
        )

        duplicates.show(truncate=False)

        (
            batch_df.write
            .format("jdbc")
            .options(**self.postgres_config)
            .mode("append")
            .save()
        )


    def load(self, df):
        return (
            df.writeStream
                .foreachBatch(self.write_to_postgres)
                .option("checkpointLocation", f"{self.checpoint_dir}/orders")
                .start()
            )

    def run(self):
        logging.info("starting pipeline....")
        extract = self.extract()
        transform = self.transform(extract)
        sQuery = self.load(transform)
        return sQuery 

        


test = Bronze()
query = test.run()
query.awaitTermination()