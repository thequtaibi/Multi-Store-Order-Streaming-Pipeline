from sqlalchemy import text
from messaging.kafka_client import KafkaConsumer

class DatabaseSender:
    def __init__(self, topic_name) -> None:
        self.topic_name = topic_name
        self.kafka_producer = KafkaConsumer(self.topic_name)

    def read_kafka(self):
        return self.kafka_producer.consum()


    def send(self, engine) -> None:

        orders = self.read_kafka()

        query = text("""
            INSERT INTO orders (
                order_id, customer_id, store_id,
                product_id, product_name, product_category, price, quantity,
                payment_status, "timestamp", order_approval,
                order_rejection_reason, item_approval, item_rejection_reason
            ) VALUES (
                :order_id, :customer_id, :store_id,
                :product_id, :product_name, :product_category, :price, :quantity,
                :payment_status, :timestamp, :order_approval,
                :order_rejection_reason, :item_approval, :item_rejection_reason
            )
        """)
        

        for order in orders:
            items = order.get("orders_itemas")
            if not isinstance(items, list) or not items:
                items = [None]


            with engine.begin() as conn:
                for item in items:
                    if not isinstance(item, dict):
                        item = {}

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
                    conn.execute(query, params)


