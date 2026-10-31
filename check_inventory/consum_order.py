from sqlalchemy import URL, create_engine, text
from classes.kafka import KafkaProducer , KafkaConsumer
import logging

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s: %(message)s"
        )


"""TODO
Require an order ID, 
valid product IDs, and positive integer quantities. 
A negative quantity currently could increase stock through your subtraction query.

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
    

        if order['order_id'] is None:
            order["approval"] = False
            order["rejection_reason"] = "missing_order_id"
        elif len(order['orders_itemas']) < 1:
            order["approval"] = False
            order["rejection_reason"] = "no_product/s"
        else:
            order["approval"] = True
            order["rejection_reason"] = None


        # for product in order['orders_itemas']:
        #     if not isinstance(product["quantity"], int) and product["quantity"] <= 0:
        #         order["approval"] = False
        #         order["rejection_reason"] = f"wrong_quantity_value={product["quantity"]}"
        
        return order

    def validation_product(self, product):
        if isinstance(product["quantity"], int) and product["quantity"] > 0:
            return True
        else:
            return False



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
                                order["rejection_reason"] = None
                            else:
                                product['approval'] = False
                                order["rejection_reason"] = "out_of_stock"
                        else:
                            product["approval"] = False
                            product["rejection_reason"] = f"wrong_quantity_value={product["quantity"]}"
                
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





