from .generate_orders import GenerateOreders
import random
from messaging.kafka_client import KafkaProducer


class OrderRequestToKafka:
    def __init__(self, store_id, currency, topic_name):
        self.store_id = store_id
        self.currency = currency
        self.kafka_producer = KafkaProducer(topic_name)


    def run(self):
        orders = []
        for i in range(1, random.randrange(10)):
            order = GenerateOreders(self.store_id, self.currency).make_order()
            orders.append(order)

        self.kafka_producer.send(orders)
        

