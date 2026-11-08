# databricks notebook



from pyspark.sql import functions as F


class Silver:
    def __init__(self):
        self.silver_dir = "orders.silver"
        self.checkpoint = "/Volumes/orders/silver/checkpoint"

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
                  .withColumn('currency', 
                              F.when(F.col("price").startswith("SAR"), "SAR")
                              .when(F.col("price").startswith("$"), "USD")
                              .when(F.col("price").startswith("€"), "EUR"))
                  
                  .withColumn('price', F.round(F.regexp_replace(F.col("price"), r"^(SAR|\$|€)\s*", ""),2))
                  .withColumn('price', F.col("price").cast("double"))
                  .withColumnRenamed('order_id', 'order_number')
                  .withColumn('total_price', F.col("price") * F.col("quantity"))
                  
                  .withColumn('order_approval',
                              F.when(
                                  (F.col("payment_status") == "approved")
                                  & (F.col("order_approval") == "true")
                                  , True
                              ).otherwise(False))
                  .withColumn('item_approval',
                              F.when(
                                  (F.col("payment_status") == "approved")
                                  & (F.col("item_approval") == "true")
                                  , True
                              ).otherwise(False))
                  
                  .withColumn('order_date', F.to_date(F.from_unixtime(F.col("timestamp"))))
                  
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
                      "currency",
                      "order_date",
                      "order_approval",
                      "item_approval",
                  ))
        
        return orders


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
            "currency",
            "order_approval",
            "item_approval"
        )



    def orders_load(self, df):
        return (
            df.writeStream
                .format("delta")
                .queryName(f"silver_ingestion_orders")
                .option("checkpointLocation", f"{self.checkpoint}/orders")
                .outputMode("append")
                .toTable(f"{self.silver_dir}.orders")
        )

    def product_load(self, df):
        return (
            df.writeStream
                .queryName('silver_ingestion_product')
                .option("checkpointLocation", f"{self.checkpoint}/product")
                .foreachBatch(self.insert_new_products)
                .start()
            )


    def orders_pipeline(self):
        raw_df = self.extract_from_bronze()
        cleaned_df = self.cleaning_and_tranformation(raw_df)
        orders_df = self.orders_df(cleaned_df)
        sQuery_orders_df = self.orders_load(orders_df)

        return sQuery_orders_df
    
    



def main() -> None:
    silver = Silver()

    sQuery_orders_df = silver.orders_pipeline()
    sQuery_orders_df.awaitTermination()



main()