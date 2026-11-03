import random
from .products_details.product_catalog import products
import time
from uuid import uuid4
import logging



"""TODO
- Add Currency
- make the data dirty
"""

class GenerateOreders:
    def __init__(self, store_id, currency):
        self.store_id = store_id
        self.currency = currency
        self.customer_id = f"CUST{random.randrange(1,9999):06d}"
        
    def add_product(self, product_id) -> dict:
        """ create product details 
        
        Return:
            dict : details of single product
        
        """

        product = products[product_id]
        
        return {
            "product_id" : product_id,
            "product_name" : product['name'],
            "product_category" : product["category"],
            "price" : f"{self.currency}{product['price']}",
            "quantity" : random.randrange(1,4)
        }

    def make_order(self) -> dict:
        """ create sinlge basket with multiple products.

        Retrun:
            dict: basket with the details of the orders.
    
        """
        orders = []
        for i in range(random.randint(1, 5)):
            product_id = f"PROD{random.randrange(1, 101):04d}"
            orders.append(self.add_product(product_id))


        

        basket = {
            "order_id" : str(uuid4()),
            "customer_id": self.customer_id,
            "store_id" : self.store_id,
            "orders_itemas" : orders,
            "payment_status":  "approved" if random.random() < 0.9 else "declined",
            "timestamp" : time.time()
        }


        return basket


if __name__ == "__main__":
    orders = GenerateOreders("store_1", "$")
    print(orders.make_order())















