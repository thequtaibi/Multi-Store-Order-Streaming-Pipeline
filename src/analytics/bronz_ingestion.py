from pyspark.sql.functions import from_json, col

class IngestionData:
    def __init__(self):
        self.landing_dir = "/Volumes/test/bronze/landing/new_orders/"
        self.checkpoint = "/Volumes/test/bronze/checkpoint/kafka_to_json/"
        self.kafka_conf = {
            "kafka.bootstrap.servers": "pkc-30550.qatarcentral.azure.confluent.cloud:9092",
            "subscribe": "orders",
            "startingOffsets": "earliest",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": (
                "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
                f'required username="XGHEHXEMAZQYC2KF" password="cfltHgWM/vc/nHn2OjiklhgXuIUdyYm8iF1sUbqjroN5K3ZAz1z/myO44aRgQH2A";'
                ),
        }
    
    def schema(self):
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


    def extarct_from_kafka(self):
        print("starting pulling form kafka")
        return spark.readStream.format("kafka").options(**self.kafka_conf).load()
    

    def tranform_to_json(self, df_kafka):
        print("starting tranform_to_json")
        return (
            df_kafka.select(
                from_json(col("value").cast("string"), self.schema()).alias("order"))
            .select("order.*")
            )
    
    def load_to_json(self, df_json):
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
    
    def run_pipeline(self):
        print("starting...")
        df_kafa = self.extarct_from_kafka()
        df = self.tranform_to_json(df_kafa)
        sQuery = self.load_to_json(df)
        return sQuery

ingestion = IngestionData()
sQuery = ingestion.run_pipeline()
sQuery.awaitTermination()
