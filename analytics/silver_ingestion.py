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

"""TODO
Validate records before writing silver.
"""

class Silver:
    def __init__(self):
        self.silver_dir = "orders_project.silver"
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
                StructField("payment_status", StringType(), True),
                StructField("approval", StringType(), True),
                StructField("rejection_reason", StringType(), True),

            ])

    def transform(self, df_raw):
        orders = (
            df_raw
            .select(from_json(col("value"), self.schema()).alias("order"))
            .select("order.*")
            .withColumn("item", explode(col("orders_itemas")))
            .select(
                "*",
                col("item.product_id").alias("product_id"),
                col("item.product_name").alias("product_name"),
                col("item.product_category").alias("product_category"),
                col("item.price").alias("price"),
                col("item.quantity").alias("quantity"),
            )
            .withColumn("total_price",col("price") * col("quantity"))
            .withColumn("date", timestamp_seconds(col("timestamp")))
            .drop("timestamp", "orders_itemas", "item")
        )

        approval_orders_df = (orders
                              .filter(col('approval') == 'true')
                              .withColumn("total_price",col("price") * col("quantity"))
                              .select(
                                  col('order_id'),
                                  col('store_id'),
                                  col('customer_id'),
                                  col('product_id'),
                                  col('product_name'),
                                  col('product_category'),
                                  col('price'),
                                  col('quantity'),
                                  col('total_price'),
                                  col('payment_status'),
                                  col('date')
                                  )
                              )
        
        rejected_orders_df = (orders
                              .filter(col('approval') == 'false')
                              .select('*'))
        return approval_orders_df, rejected_orders_df
    
    def rejeced_order_load(self, df):
        sQuery = (df.writeStream
                  .queryName('silver_ingestion_rejeced_order_load')
                  .option('checkpointLocation', f"{self.check_point}/approved")
                  .outputMode('append')
                  .trigger(availableNow=True)
                  .toTable(f"{self.silver_dir}.rejected_orders")
                  )
        return sQuery

    def approval_orders_load(self, clean_df):
        sQuery = (clean_df.writeStream
                  .queryName('silver_ingestion_approval_orders_load')
                  .option('checkpointLocation', f"{self.check_point}/rejected")
                  .outputMode('append')
                  .trigger(processingTime="30 seconds")
                  .toTable(f"{self.silver_dir}.approval_orders")
                  )
        return sQuery

    def run(self):
        df_raw = self.extract()
        approval_orders, rejeced_order = self.transform(df_raw)

        sQuery_approval = self.approval_orders_load(approval_orders)
        sQuery_rejeced = self.rejeced_order_load(rejeced_order)

        return sQuery_approval, sQuery_rejeced

