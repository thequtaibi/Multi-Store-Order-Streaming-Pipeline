from pyspark.sql import functions as F


class Silver:
    def __init__(self):
        self.silver_dir = "orders.silver"
        self.checkpoint = "/Volumes/orders/silver/checkpoint/orders_source"

        # Must match your Bronze writer's output path.
        self.bronze_landing = "/Volumes/orders/bronze/landing/new_orders/"

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
            timestamp DOUBLE,
            order_approval STRING,
            order_rejection_reason STRING,
            item_approval STRING,
            item_rejection_reason STRING,
            sent_to_kafka STRING
        """

    def extract_from_bronze(self):
        return (
            spark.readStream
            .schema(self.schema())
            .json(f"{self.bronze_landing}*.json")
        )
    
    def cleaning_and_tranformation(self, df):
        orders = (df
                  .withColumn('price', F.regexp_replace(F.col("price"), r"^(SAR|\$|€)\s*", ""))
                  .withColumnRename(F.col('order_id'), 'order_number')
                  .withColumn('total_price', F.col("price") * F.col("quantity"))
                  .wihtColumn('approval',
                              F.when(
                                  (F.col("payment_status") == "approved")
                                  & (F.col("order_approval") == "true")
                                  & (F.col("item_approval") == "true")
                                  , True
                              ).otherwise(False))
                  .withCOlumn('order_date', F.to_date(F.col("timestamp")))
                  .select(
                      "enent_id",
                      "order_number",
                      "store_id",
                      "customer_id",
                      "product_id",
                      "product_name",
                      "product_category",
                      "price",
                      "quantity",
                      "total_price",
                      "order_date",
                      "approval",
                  ))
        
        return orders

    def product_df(self, orders):
        return orders.select(
            "product_id",
            "product_name",
            "product_category"
        )
    
    def orders_df(self, orders):
        return orders.select(
             "enent_id",
            "order_number",
            "store_id",
            "customer_id",
            "product_id",
            "price",
            "quantity",
            "total_price",
            "order_date",
            "approval"
        )
                  

    def load(self, df_name , df):
        return (
            df.writeStream
            .format("delta")
            .queryName(f"silver_ingestion_{df_name}")
            .option(
                "checkpointLocation",
                f"{self.checkpoint}/{df_name}"
            )
            .outputMode("append")
            .toTable(f"{self.silver_dir}.{df_name}")
        )



    def run_pipeline(self):
        raw_df = self.extract_from_bronze()
        cleaned_df = self.cleaning_and_tranformation(raw_df)
        product_df = self.product_df(cleaned_df)
        orders_df = self.orders_df(cleaned_df)

        sQuery_product = self.load('product_df', product_df)
        sQuery_orders_df = self.load('orders_df', orders_df)

        return sQuery_orders_df, sQuery_product


def main() -> None:
    silver = Silver()
    sQuery_orders_df, sQuery_product = silver.run_pipeline()

    sQuery_orders_df.awaitTermination()
    sQuery_product.awaitTermination()

