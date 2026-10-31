import random
from .products_details.products_details import products
import time
from uuid import uuid4
import logging
from .products_details.stories_data import stores

""" TODO
Select the product inside add_product(), so baskets can contain different products. Currently, it’s selected once in the constructor.
it repate the same order 
"""


class GenerateOreders:
    def __init__(self):
        # self.product_id = f"PROD{random.randrange(1, 101):04d}"
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
            "price" : product['price'],
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
            "store_id" : random.choice(stores),
            "orders_itemas" : orders,
            "timestamp" : time.time()
        }


        return basket


if __name__ == "__main__":
    orders = GenerateOreders()
    print(orders.make_order())















