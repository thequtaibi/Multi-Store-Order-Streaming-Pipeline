from sqlalchemy import text
from messaging.database_con import DatabaseConnector
from messaging.kafka_client import KafkaConsumer
import json

class DatabaseSender:
    def __init__(self, drivername, username, password, host, port, database, topic_name):
        self.database_connector = DatabaseConnector(
            drivername=drivername,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database
        )
        self.topic_name = topic_name
        self.kafka_producer = KafkaConsumer(self.topic_name)

    def read_kafka(self, topic_name):
        orderas = self.kafka_producer.consum()


    def send(self):
        engine = self.database_connector()

        orders = self.read_kafka()

        query = text("""
            MERGE INTO orders AS target
            USING (
                SELECT CAST(:order_id AS UUID) AS order_id
            ) AS source
            ON target.order_id = source.order_id

            WHEN NOT MATCHED THEN
                INSERT (
                    order_id,
                    customer_id,
                    store_id,
                    orders_itemas,
                    payment_status,
                    "timestamp",
                    approval,
                    rejection_reason
                )
                VALUES (
                    source.order_id,
                    :customer_id,
                    :store_id,
                    CAST(:orders_itemas AS JSONB),
                    :payment_status,
                    :timestamp,
                    :approval,
                    :rejection_reason
                )
        """)
        

        for order in orders:
            items = order.get("orders_itemas") or [None]

            for item in items:
                item = item or {}

                params = {
                    "order_id": order.get("order_id"),
                    "customer_id": order.get("customer_id"),
                    "store_id": order.get("store_id"),

                    "product_id": item.get("product_id"),
                    "product_name": item.get("product_name"),
                    "product_category": item.get("product_category"),
                    "price": item.get("price"),
                    "quantity": item.get("quantity"),

                    "payment_status": order.get("payment_status"),
                    "timestamp": order.get("timestamp"),

                    "order_approval": order.get("approval"),
                    "order_rejection_reason": order.get("rejection_reason"),
                    "item_approval": item.get("approval"),
                    "item_rejection_reason": item.get("rejection_reason"),
                }
            
            engine.execute(query, params)
            engine.commit()


