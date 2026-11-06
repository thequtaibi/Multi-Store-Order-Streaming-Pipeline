from order_generation.order_producer import OrderRequestToKafka
from inventory.order_consumer import OrderValidation
from db.db_sender import DatabaseSender
from db.db_connector import DatabaseConnector
from messaging.db_to_kafka import SendKafka
import time
import random

class StoreSimulation:
    def __init__(self, store_id, currency, receive_topic, send_topic) -> None:
        self.store_id = store_id
        self.currency = currency
        self.receive_topic = receive_topic
        self.send_topic = send_topic
        
        self.engine = DatabaseConnector(
                drivername="postgresql+psycopg",
                username="khlk4004",
                password="4004",
                host="localhost",
                port=5432,
                database=self.store_id,
            ).database_connection()
        
        self.gen_order_send_kafka = OrderRequestToKafka(store_id=self.store_id, currency=self.currency, topic_name=self.receive_topic)    
        self.validator = OrderValidation(receive_topic, self.send_topic)
        self.database_sender = DatabaseSender(self.send_topic)
        self.send_from_database_to_kafka = SendKafka(topic_name="orders", engine= self.engine)


    def generate_order(self) -> None:
        """ generate orders and send them to kafka topic = order_request_sx
        """
        self.gen_order_send_kafka.run()


    def vaildate_order(self)-> None:
        """ vaild the order and send them to another kafka topic = orders_result_sx
        """
        self.validator.run(self.engine)

    def send_to_database(self) -> None:
        """ pull from orders_result_sx and add them to orders table in the database 
        """
        self.database_sender.send(self.engine)

    def run(self) -> None:
        self.generate_order()
        self.vaildate_order()
        self.send_to_database()

        # send the orders from db of each store and send them to kakfa topic = orders
        self.send_from_database_to_kafka.run()


    def close(self):
        try:
            self.validator.receive_data.consumer.close()
        finally:
            self.database_sender.kafka_producer.consumer.close()



def main() -> None:
    store_1 = StoreSimulation(
            store_id="store_1",
            currency="$",
            receive_topic="order_request_s1",
            send_topic="orders_result_s1",
        )
    
    store_2 = StoreSimulation(
        store_id="store_2",
        currency="SAR",
        receive_topic="order_request_s2",
        send_topic="orders_result_s2",
    )

    store_3 = StoreSimulation(
        store_id="store_3",
        currency="€",
        receive_topic="order_request_s3",
        send_topic="orders_result_s3",
    )

    stores = [store_1, store_2, store_3]

    try:
        while True:
            for store in stores:
                store.run()

            time.sleep(random.randrange(1, 30))

    except KeyboardInterrupt:
        print("Simulation stopped")

    finally:
        for store in stores:
            store.close()
    
        

if __name__ == "__main__":
    main()
