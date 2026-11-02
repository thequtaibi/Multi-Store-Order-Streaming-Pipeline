from messaging.database_con import DatabaseConnector
from sqlalchemy import text
from messaging.kafka_client import KafkaProducer

class DatabaseToKafka:
    def __init__(self, drivername, username, password, host, port, database, topic_name):
        self.database_connector = DatabaseConnector(
            drivername=drivername,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
        )
        self.kafka_producer = KafkaProducer(topic_name)



    def pull_data(self):
        engine = self.database_connector.database_connection()

        with engine.begin() as conn:
            reuslts = conn.execute(
                text("""
                    SELECT *
                    FROM orders
                    WHERE sent_to_kafka = FALSE
                    ORDER BY order_id
                    LIMIT 100;
                    """)
                )
        return reuslts

    def update_database(self, reuslts):
        engine = self.database_connector.database_connection()
        for reuslt in reuslts:
             
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE orders
                        SET sent_to_kafka = TRUE
                        WHERE order_id = :order_id;
                        """),{
                            "order_id" : reuslt['order_id']
                        }
                    )
        


    def send_data(self, reuslt):
        try:
            self.kafka_producer.send(reuslt)
            self.update_database(reuslt)
        except BufferError:
            print('something')

            


