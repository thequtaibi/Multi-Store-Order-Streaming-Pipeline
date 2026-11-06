from pyspark.sql import functions as F

def update_gold(batch_df, batch_id):
    session = batch_df.sparkSession

    all_items = session.table("orders.silver.approval_orders")

    summary = (
        all_items
        .groupBy("store_id")
        .agg(
            F.count_distinct("order_id")
                .alias("number_of_orders"),

            F.sum(F.col("price") * F.col("quantity"))
                .alias("total_revenue"),

            F.sum("quantity")
                .alias("total_units_sold")
        )
    )

    (
        summary.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("orders.gold.store_summary")
    )


query = (
    spark.readStream
    .table("orders.silver.approval_orders")
    .writeStream
    .foreachBatch(update_gold)
    .option(
        "checkpointLocation",
        "/Volumes/orders/gold/checkpoint/store_summary"
    )
    .start()
)