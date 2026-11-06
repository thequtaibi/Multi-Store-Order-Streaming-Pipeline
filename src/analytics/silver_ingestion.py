from pyspark.sql.functions import col, from_json, timestamp_seconds, explode, regexp_replace
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
        self.silver_dir = "orders.silver"
        self.check_point = "/Volumes/orders/silver/checkpoin"
        

    def extract(self):
        return spark.readStream.table('orders.bronze.bronz_order')

    def schema(self):
        return StructType([
                StructField("enent_id", StringType(), True),
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("store_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("product_category", StringType(), True),
                StructField("price", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("payment_status", StringType(), True),
                StructField("timestamp", DoubleType(), True),
                StructField("order_approval", StringType(), True),
                StructField("order_rejection_reason", StringType(), True),
                StructField("item_approval", StringType(), True),
                StructField("item_rejection_reason", StringType(), True),
            ])

    def transform(self, df_raw):
        orders = (
            df_raw
            .select(from_json(col("value"), self.schema()).alias("order"))
            .select("order.*")
            .withColumn(
                "price",
                regexp_replace(col("price"), r"[$€]|SAR", "")
                .cast(DecimalType(10, 2))
            )
            .withColumn("total_price",col("price") * col("quantity"))
            .withColumn("date", timestamp_seconds(col("timestamp")))
            .drop("timestamp", "orders_itemas", "item")
        )

        approval_orders_df = (orders
                              .filter(col('order_approval') == 'true')
                              .withColumn("total_price",col("price") * col("quantity"))
                              .select(
                                  col('enent_id'),
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
                              .filter(col('order_approval') == 'false')
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



bronze_query_approval, bronze_query_rejeced  = Silver().run()
bronze_query_approval.awaitTermination()
bronze_query_rejeced.awaitTermination()
