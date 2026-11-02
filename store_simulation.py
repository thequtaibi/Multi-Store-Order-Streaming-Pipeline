from order_generation.order_producer import OrderRequestToKafka
from inventory.order_consumer import OrderValidation
from database_sender.database_sender import DatabaseSender
from messaging.database_con import DatabaseConnector

class StoreSimulation:
    def __init__(self, store_id, receive_topic, send_topic):
        self.store_id = store_id
        self.engine = DatabaseConnector(
                drivername="postgresql+psycopg",
                username="khlk4004",
                password="4004",
                host="localhost",
                port=5432,
                database=self.store_id,
            ).database_connection()
        self.validator = OrderValidation(receive_topic, send_topic)
        self.database_sender = DatabaseSender(send_topic)



    

    def generate_order(self, store_id, topic_name):
        OrderRequestToKafka(topic_name).run(store_id)


    def vaildate_order(self):
        self.validator.run(self.engine)

    def send_to_database(self):
        self.database_sender.send(self.engine)


if __name__ == "__main__":
    store1 = StoreSimulation('store_1', 'order_request_s1', 'orders_result_s1')

    try:
        while True:
            store1.generate_order("store1", "order_request_s1")
            store1.vaildate_order()
            store1.send_to_database()
    except KeyboardInterrupt:
            print("Consumer stopped")
    finally:
        try:
            store1.validator.receive_data.consumer.close()
        finally:
            store1.database_sender.kafka_producer.consumer.close()
    
