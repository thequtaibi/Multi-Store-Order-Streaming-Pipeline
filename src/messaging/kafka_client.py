from confluent_kafka import Consumer, Producer
import json
import logging


logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s: %(message)s"
        )

class KafkaProducer:
    def __init__(self, topic_name) -> None:
        self.delivery = {"success": None}
        self.topic = topic_name
        self.config = {
            'bootstrap.servers': "pkc-30550.qatarcentral.azure.confluent.cloud:9092",
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': "XGHEHXEMAZQYC2KF",
            'sasl.password': "cfltHgWM/vc/nHn2OjiklhgXuIUdyYm8iF1sUbqjroN5K3ZAz1z/myO44aRgQH2A",
            'client.id': 'store',
        }
        self.producer = Producer(self.config)


    def _delivery_callback(self, erorr, message) -> None:
            if erorr:
                self.delivery["success"] = False
                logging.error(f"Delivery Faild {erorr}")
            else:
                self.delivery["success"] = True
                logging.info(
                    f"Delivery to {message.key()} "
                    f"partition = {message.topic()} "
                    f"offset = {message.offset()} "
                )


    def send(self, orders):
        if isinstance(orders, dict):
            orders = [orders]

        for order in orders:
            
            if not None:
                order_id = order.get("order_id")
                try:
                    self.producer.produce(
                        self.topic,
                        key=str(order_id) if order_id else None,
                        value=json.dumps(order, default=str).encode("utf-8"),
                        callback=self._delivery_callback
                    )
                except BufferError:
                    logging.error("Producer buffer full for order_id=%s", order_id)
                    self.producer.poll(1)
                self.producer.poll(0)
                self.producer.flush()


class KafkaConsumer:
    def __init__(self, topic_name) -> None:
        self.config = {
                'bootstrap.servers': "pkc-30550.qatarcentral.azure.confluent.cloud:9092",
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': "XGHEHXEMAZQYC2KF",
                'sasl.password': "cfltHgWM/vc/nHn2OjiklhgXuIUdyYm8iF1sUbqjroN5K3ZAz1z/myO44aRgQH2A",                    
                'group.id': 'orders_debug_02',
                'auto.offset.reset': 'earliest'
                }
        self.consumer = Consumer(self.config)
        self.consumer.subscribe([topic_name])


    def consum(self) -> json:
        msg = self.consumer.poll(1.0)
        if msg is None:
            return []

        if msg.error():
            print(f"Kafka error: {msg.error()}")
            return []

        return [json.loads(msg.value().decode("utf-8"))]