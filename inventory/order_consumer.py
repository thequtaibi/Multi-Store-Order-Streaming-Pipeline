from sqlalchemy import URL, create_engine, text
from messaging.kafka_client import KafkaProducer , KafkaConsumer
import logging

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s: %(message)s"
        )


"""TODO
create database for orders
and check for duplcated orders by connected the database table
"""

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


    def validation_order(self, order):
        items = order.get("orders_itemas")
        payment_status = order.get('payment_status')
        if not order.get("order_id"):
            order["approval"] = False
            order["rejection_reason"] = "missing order id"

        elif not isinstance(items, list) or not items:
            order["approval"] = False
            order["rejection_reason"] = "missing or invalid items"

        elif payment_status == 'declined' or payment_status is None:
            order["approval"] = False
            order["rejection_reason"] = "payment state declined"

        else:
            order["approval"] = True
            order["rejection_reason"] = None

        
        return order


    def validation_product(self, product):
        product_id = product.get("product_id") # return None of there's no product_id to void erorr
        quantity = product.get("quantity")

        return ( 
                isinstance(product_id, str)
                and type(quantity) is int 
                and quantity > 0
        )


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


    def run(self):
            orders = self.receive_data.consum()

            for order in orders:
                self.validation_order(order)
                if order['approval']:
                    for product in order["orders_itemas"]:
                        
                        if self.validation_product(product):
                            message = self.update_database(product)
        
                            if message:
                                product['approval'] = True
                                product["rejection_reason"] = None
                            else:
                                product['approval'] = False
                                product["rejection_reason"] = "out_of_stock"
                        else:
                            product["approval"] = False
                            product["rejection_reason"] = "invalid_product_id_or_quantity"
                
                self.send_data.send(order)
    

if __name__ == "__main__":
    order_validation = OrderValidation()

    try:
        while True:
            orders = order_validation.run()

    except KeyboardInterrupt:
        print("Consumer stopped")

    finally:
        order_validation.receive_data.consumer.close()
