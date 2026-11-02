from order_generation.order_producer import OrderRequestToKafka
from inventory.order_consumer import OrderValidation
from database_sender.database_sender import DatabaseSender
from messaging.database_con import DatabaseConnector

class StoreSimulation:
    def __init__(self, store_id, receive_topic, send_topic):
        self.store_id = store_id
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
        self.validator = OrderValidation(receive_topic, self.send_topic)
        self.database_sender = DatabaseSender(self.send_topic)



    

    def generate_order(self, store_id, topic_name):
        OrderRequestToKafka(topic_name).run(store_id)


    def vaildate_order(self):
        self.validator.run(self.engine)

    def send_to_database(self):
        self.database_sender.send(self.engine)

    def run(self):
        try:
            while True:
                self.generate_order(self.store_id, self.receive_topic)
                self.vaildate_order()
                self.send_to_database()
        except KeyboardInterrupt:
                print("Simulation stopped")
        finally:
            try:
                self.validator.receive_data.consumer.close()
            finally:
                self.database_sender.kafka_producer.consumer.close()


if __name__ == "__main__":
    store_id = StoreSimulation(store_id="store_1", receive_topic="order_request_s1", send_topic="orders_result_s1")
    store_id.run()

    
