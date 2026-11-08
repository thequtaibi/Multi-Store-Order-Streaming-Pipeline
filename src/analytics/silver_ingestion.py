import json
import os

# databricks notebook


from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col

def kafka_options():
    """Read credentials from the Databricks runtime environment."""
    names = (
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_SASL_USERNAME",
        "KAFKA_SASL_PASSWORD",
    )
    missing = [name for name in names if not os.environ.get(name, "").strip()]
    if missing:
        raise ValueError("Missing Kafka environment variables: " + ", ".join(missing))

    username = json.dumps(os.environ["KAFKA_SASL_USERNAME"], ensure_ascii=False)
    password = json.dumps(os.environ["KAFKA_SASL_PASSWORD"], ensure_ascii=False)
    return {
        "kafka.bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "subscribe": "orders",
        "startingOffsets": "earliest",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
            f"required username={username} password={password};"
        ),
    }


class Bronze:
    def __init__(self) -> None:
        self.landing_dir = "/Volumes/orders/bronze/landing/new_orders/"
        self.checkpoint = "/Volumes/orders/bronze/checkpoint/kafka_to_json/"
        self.kafka_conf = kafka_options()
    
    def _schema(self) -> any:
        return """
                enent_id STRING,
                order_id STRING,
                customer_id STRING,
                store_id STRING,
                product_id STRING,
                product_name STRING,
                product_category STRING,
                price STRING,
                quantity INT,
                payment_status STRING,
                timestamp Double,
                order_approval STRING,
                order_rejection_reason STRING,
                item_approval STRING,
                item_rejection_reason STRING,
                sent_to_kafka STRING
            """

    def extarct_from_kafka(self) -> DataFrame:
        return spark.readStream.format("kafka").options(**self.kafka_conf).load()
    

    def tranform_to_json(self, df_kafka) -> DataFrame:
        return (
            df_kafka
            .select(from_json(col("value").cast("string"), self._schema()).alias("order"))
            .select("order.*")
            )

    
    def load_to_json(self, df_json) -> any:
        print("starting load_to_json")
        sQuery = (
            df_json.writeStream
            .format("json")
            .queryName("ingestion_data_from_kafka")
            .outputMode('append')
            .option("maxRecordsPerFile", 1) # make 1 file for each order
            .option('path', self.landing_dir)
            .option('checkpointLocation', self.checkpoint)
            # .trigger(processingTime="10 seconds")
            .start()
            )
        
        return sQuery

    
    def run_pipeline(self) -> any:
        print("starting...")
        df_kafa = self.extarct_from_kafka()
        df = self.tranform_to_json(df_kafa)
        sQuery = self.load_to_json(df)
        return sQuery

def main() -> None:
    ingestion = Bronze()
    sQuery = ingestion.run_pipeline()
    sQuery.awaitTermination()

main()
