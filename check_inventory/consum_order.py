from sqlalchemy import URL, create_engine, text
from classes.kafka import KafkaProducer , KafkaConsumer
import logging

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s: %(message)s"
        )


DATABASE_URL = URL.create(
            drivername="postgresql+psycopg",
            username="khlk4004",
            password="4004",
            host="localhost",
            port=5432,
            database="ecommerce_practice"
)
engine = create_engine(DATABASE_URL)


class OrderValidation:
    def __init__(self):
        self.receive_data = KafkaConsumer('order_request')
        self.send_data = KafkaProducer('orders')

    def checking_database(self):
            orders = self.receive_data.consum()
    
            for order in orders:
                for product in order["orders_itemas"]:
    
                    # checking and update the database
                    message = self.update_database(product)
    
                    if message:
                        product['approval'] = True
                    else:
                        product['approval'] = False

                
                self.send_data.send(order)
                        



    def update_database(self, product):
        with engine.begin() as conn:
            result = conn.execute(
                text(
                """
                    UPDATE products 
                    SET  quantity = quantity - :quantity 
                    WHERE product_id = :product_id AND quantity >= :quantity
                    """
                ),
                {
                    "product_id":product["product_id"],
                    "quantity":product["quantity"] 
                }
            )
        return result.rowcount == 1

    

if __name__ == "__main__":
    order_validation = OrderValidation()

    try:
        while True:
            orders = order_validation.checking_database()

    except KeyboardInterrupt:
        print("Consumer stopped")

    finally:
        order_validation.receive_data.consumer.close()





