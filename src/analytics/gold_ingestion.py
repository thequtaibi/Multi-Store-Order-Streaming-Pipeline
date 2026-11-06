from pyspark.sql.functions import col

class Gold:
    def __init__(self, store_id):
        self.store_id = store_id
        self.gold_dir = "orders.gold"

    def extract(self):
        return spark.readStream.table('orders.silver.approval_orders')

    def tranform(self, df):
        return df.filter(col("store_id") == self.store_id)

    def load(self, store_df):
        sQuery = (store_df.writeStream
                  .queryName(self.store_id)
                  .option('checkpointLocation', f"/Volumes/orders/gold/checkpoint/{self.store_id}")
                  .outputMode('append')
                  .trigger(processingTime="30 seconds")
                  .toTable(f"{self.gold_dir}.order{self.store_id}")
                  )
        return sQuery
    
    def run(self):
        df = self.extract()
        store_1_df = self.tranform(df)
        return self.load(store_1_df)

store_1 = Gold('store_1').run()
store_2 = Gold('store_2').run()
store_3 = Gold('store_3').run()


try:
    store_1.awaitTermination()
    store_2.awaitTermination()
    store_3.awaitTermination()
finally:
    store_1.stop()
    store_2.stop()
    store_3.stop()