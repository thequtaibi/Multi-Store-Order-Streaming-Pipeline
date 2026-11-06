from pyspark.sql.functions import (
    col,
    coalesce,
    expr,
    lit,
    lower,
    regexp_replace,
    timestamp_seconds,
    trim,
    upper,
    when,
)


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

    def transform(self, df):


        orders = (
            df
            .withColumn("original_price", col("price"))
            .withColumn(
                "currency",
                when(col("price").startswith("SAR"), "SAR")
                .when(col("price").startswith("$"), "USD")
                .when(col("price").startswith("€"), "EUR")
                .otherwise("UNKNOWN")
            )
            .withColumn(
                "price",
                trim(
                    regexp_replace(
                        col("price"),
                        r"^(SAR|\$|€)\s*",
                        ""
                    )
                )
            )
            .withColumn(
                "price",
                expr("try_cast(price AS DECIMAL(18, 2))")
            )
            .withColumn(
                "total_price",
                col("price") * col("quantity")
            )
            .withColumn(
                "date",
                timestamp_seconds(col("timestamp"))
            )
            .drop("timestamp")
        )

        

        both_approved = (
            (col("order_approval") == "true")
            & (col("item_approval") == "true")
        )

        approved_orders = (
            orders.filter(
            (col('order_approval') == 'true') & (col('payment_status') == 'approved') | (col('item_approval') == 'true'))
            .select(
                col('enent_id'),
                col('order_id'),
                col('customer_id'),
                col('store_id'),
                col('product_id'),
                col('product_name'),
                col('product_category'),
                col('price'),
                col('quantity'),
                col('total_price'),
                col('date'))
            )

        

        rejected_orders = (orders.filter(
            (col('order_approval') == 'false') & (col('payment_status') == 'rejected') | (col('item_approval') == 'false')
        )
        .select("*")
        )

        return approved_orders, rejected_orders

    def approved_orders_load(self, df):
        return (
            df.writeStream
            .format("delta")
            .queryName("silver_ingestion_approved")
            .option(
                "checkpointLocation",
                f"{self.checkpoint}/approved"
            )
            .outputMode("append")
            .toTable(f"{self.silver_dir}.approval_orders")
        )

    def rejected_orders_load(self, df):
        return (
            df.writeStream
            .format("delta")
            .queryName("silver_ingestion_rejected")
            .option(
                "checkpointLocation",
                f"{self.checkpoint}/rejected"
            )
            .outputMode("append")
            .toTable(f"{self.silver_dir}.rejected_orders")
        )

    def run(self):
        df_raw = self.extract_from_bronze()

        approved_orders, rejected_orders = self.transform(df_raw)

        approved_query = self.approved_orders_load(approved_orders)
        rejected_query = self.rejected_orders_load(rejected_orders)

        return approved_query, rejected_query




approved_query, rejected_query = Silver().run()

approved_query.awaitTermination()
rejected_query.awaitTermination()