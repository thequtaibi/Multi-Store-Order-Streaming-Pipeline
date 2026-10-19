from confluent_kafka import Consumer
from sqlalchemy import URL, create_engine, text
import json
"""
TODO
- consume the order from kafka
- choice availablety
- sand the message to orders topic
"""

DATABASE_URL = URL.create(
            drivername="postgresql+psycopg",
            username="khlk4004",
            password="4004",
            host="localhost",
            port=5432,
            database="ecommerce_practice"
)


class ORsderVildation:
    def __init__(self):
        
        self.config = {
            'bootstrap.servers': "pkc-30550.qatarcentral.azure.confluent.cloud:9092",
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': "XGHEHXEMAZQYC2KF",
            'sasl.password': "cfltHgWM/vc/nHn2OjiklhgXuIUdyYm8iF1sUbqjroN5K3ZAz1z/myO44aRgQH2A",
            'group.id': 'orders_consumer_group',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.config)
        engine = create_engine(self.DATABASE_URL)
        

    def resaive_order(self):
        msg = self.consumer.pause(1)

        if msg is None:
            pass

        if msg.error():
            print(f"Kafka error: {msg.error()}")

        return [json.loads(msg.value().decode('utf-8'))]


    def checking_database(orders):
        engine = create_engine(DATABASE_URL)
        orders = {}

        for order in orders:
            product_id = order["product_id"]
            order_quantity = order["quantity"]

            with engine.begin() as conn:
                result = conn.execute(
                    text("""
                        SELECT 
                            quantity
                        FROM product
                        WHERE product_id = :product_id
                    """),
                        {
                            "product_id":product_id 
                        })
                available_quantity = result.mappings().first()
                if int(available_quantity) >= order_quantity:
                    message = 'approved'
                else:
                    message = 'rejected'
                
                order["vildation"] = message
                







