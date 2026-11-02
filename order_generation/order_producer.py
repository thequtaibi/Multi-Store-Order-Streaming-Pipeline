
from .generate_orders import GenerateOreders
import random
import time
from messaging.kafka_client import KafkaProducer


class OrderRequestToKafka:
    def __init__(self, topic_name):
        self.kafka_producer = KafkaProducer()


    def run(self):
        orders = []
        for i in range(1, random.randrange(10)):
            order = GenerateOreders().make_order()
            orders.append(order)

        self.kafka_producer.send(orders)
        

if __name__ == "__main__":
    store1 = OrderRequestToKafka(topic_name="order_request_s1")
    store2 = OrderRequestToKafka(topic_name="order_request_s2")
    store3 = OrderRequestToKafka(topic_name="order_request_s3")
    while True:
        store1.run()
        store2.run()
        store3.run()
        time.sleep(random.randrange(1,random.randrange(2,60)))
    