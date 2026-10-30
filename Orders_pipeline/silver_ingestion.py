# Databricks notebook source
from pyspark.sql.functions import col, from_json, timestamp_seconds, explode
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    ArrayType,
    DoubleType
)


class Silver:
    def __init__(self):
        self.silver_dir = "orders_project.silver.clean_orders"
        self.check_point = "/Volumes/orders_project/silver/checkpoint"
        

    def extract(self):
        return spark.readStream.table('orders_project.bronze.bronz_order')

    def schema(self):
        return StructType([
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("store_id", StringType(), True),
                StructField(
                    "orders_itemas",
                    ArrayType(
                        StructType([
                            StructField("product_id", StringType(), True),
                            StructField("product_name", StringType(), True),
                            StructField("product_category", StringType(), True),
                            StructField("price", DecimalType(10, 2), True),
                            StructField("quantity", IntegerType(), True),
                            StructField("approval", StringType(), True),
                        ])
                    ),
                    True
                ),
                StructField("timestamp", DoubleType(), True),
            ])

    def tranform(self, df_raw):
        orders = (
            df_raw
            .select(from_json(col("value"), self.schema()).alias("order"))
            .select("order.*")
            .withColumn("item", explode(col("orders_itemas")))
            .select("*", "item.*")
            .withColumn("total_price",col("price") * col("quantity"))
            .withColumn("date", timestamp_seconds(col("timestamp")))
            .drop("timestamp", "orders_itemas", "item")
        )

        return orders

    def load(self, clean_df):
        sQuery = (clean_df.writeStream
                  .queryName('silver_ingestion')
                  .option('checkpointLocation', self.check_point)
                  .outputMode('append')
                  .trigger(availableNow=True)
                  .toTable(self.silver_dir)
                  )
        return sQuery

    def run(self):
        df_raw = self.extract()
        clean_df = self.tranform(df_raw)
        sQuery = self.load(clean_df)
        return sQuery

silver_query = Silver().run()
silver_query.awaitTermination()