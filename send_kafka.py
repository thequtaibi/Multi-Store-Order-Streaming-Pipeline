from messaging.database_con import DatabaseConnector
from messaging.kafka_client import KafkaProducer


class SendKafka:
    def __init__(self, store_id, topic_name):
        self.engine = DatabaseConnector(
                        drivername="postgresql+psycopg",
                        username="khlk4004",
                        password="4004",
                        host="localhost",
                        port=5432,
                        database=store_id,
                    ).database_connection()

        self.kafka_producer = KafkaProducer(topic_name)



    def read_from_database(self):
        query = """
            SELECT 
                id AS enent_id,
                order_id,
                customer_id,
                store_id,
                product_id,
                product_name,
                product_category,
                price,
                quantity,
                payment_status,
                timestamp,
                order_approval,
                order_rejection_reason,
                item_approval,
                item_rejection_reason
            FROM orders
            WHERE sent_to_kafka = FALSE
            LIMIT 100;
            """

        with self.engine.connect() as conn:
            result = conn.exec_driver_sql(query)
            return [dict(row) for row in result.mappings().all()]


    def send_pending(self):
        orders = self.read_from_database()

        for order in orders:
             self.kafka_producer.delivery["success"] = None
             self.kafka_producer.send(order)
             if self.kafka_producer.delivery["success"]:
                  self.update_database(order['enent_id'])
            

        
    def update_database(self, id):
            query = """
                UPDATE orders
                SET sent_to_kafka = TRUE
                WHERE id = %(id)s
            """

            with self.engine.begin() as conn:
                conn.exec_driver_sql(query, {"id": id})



    def run(self):
         self.read_from_database()
         self.send_pending()


    
