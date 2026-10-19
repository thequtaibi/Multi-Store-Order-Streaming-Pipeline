from confluent_kafka import Producer
from generate_orders import GenerateOreders
import logging
from dotenv import load_dotenv
import os
from pathlib import Path
import json
import random


"""
TODO
send random number of order each time
"""


ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

logging.basicConfig(
        filename='/home/kind/Desktop/project_x/make_orders/logs/producer.log',
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s: %(message)s"
        )

class OrderRequestToKafka:
    def __init__(self):
        self.topic = "order_request"
        self.config = {
            'bootstrap.servers': os.environ["bootstrap_servers"],
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': os.environ["API_KEY"],
            'sasl.password': os.environ["API_SECURE"],
            'client.id': 'store_1',
        }
        self.producer = Producer(self.config)

    def delivery_callback(self, erorr, message):
        if erorr:
            logging.error(f"Delivery Faild {erorr}")
        else:
            logging.info(
                f"Delivery to {message.topic()} "
                f"partition = {message.partition()}"
                f"offset = {message.offset()}"
            )

    def send_order_request(self, orders):

        for order in orders:
            if not None:
                try:
                    self.producer.produce(
                        self.topic,
                        key=order["order_id"],
                        value=json.dumps(order).encode("utf-8"),
                        callback=self.delivery_callback
                    )
                except BufferError:
                    logging.error(f" in order_id = {order["order_id"]}")
                    self.producer.poll(1)
                self.producer.poll(0)
                self.producer.flush()

    def run(self):
        order = GenerateOreders().make_order()
        orders = []
        for i in range(1, random.randrange(10)):
            orders.append(order)

        print(orders)
        # self.send_order_request(orders)


if __name__ == "__main__":
    OrderRequestToKafka().run()
    