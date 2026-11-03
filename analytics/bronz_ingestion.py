from pyspark.sql.functions import from_json, col



class Bronz:
    def __init__(self):
        self.bronz_dir = "orders.bronze.bronz_order"
        self.bronz_checkpint = "/Volumes/orders/bronze/checkpoint"
        self.consumer_conf = {
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
    

    def extract(self):
        return (
            spark.readStream
            .format("kafka")
            .options(**self.consumer_conf)
            .load()
        )


    def transform(self, df_kafa):
        
        messages_df = df_kafa.selectExpr(
                "CAST(key AS STRING) AS key",
                "CAST(value AS STRING) AS value",
                "topic",
                "partition",
                "offset",
                "timestamp"
            )
        
        return messages_df

    def load(self, df):
        sQuery = (
            df.writeStream
            .queryName('bronz_ingestion')
            .option('checkpointLocation', self.bronz_checkpint)
            .outputMode('append')
            .trigger(processingTime="30 seconds")
            .toTable(self.bronz_dir)
            )
        return sQuery

    def run(self):
        print('pipeline starting...')
        df_kafka = self.extract()
        df = self.transform(df_kafka)
        sQuery = self.load(df)
        return sQuery



bronze_query = Bronz().run()
bronze_query.awaitTermination()
