
from .generate_orders import GenerateOreders
import random
import time
from messaging.kafka_client import KafkaProducer


class OrderRequestToKafka:
    def __init__(self):
        self.kafka_producer = KafkaProducer("order_request")


    def run(self):
        orders = []
        for i in range(1, random.randrange(10)):
            order = GenerateOreders().make_order()
            orders.append(order)

        self.kafka_producer.send(orders)
        

if __name__ == "__main__":
    product = OrderRequestToKafka()
    while True:
        product.run()
        time.sleep(random.randrange(1,random.randrange(2,60)))
    