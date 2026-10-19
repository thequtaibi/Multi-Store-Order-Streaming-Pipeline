import random
from products_details.products_details import products
import time
from uuid import uuid4
import logging
from products_details.stories_data import stores

""" TODO
- add store_id
- add city

"""


class GenerateOreders:
    def __init__(self):
        self.product_id = f"PROD{random.randrange(1, 101):04d}"
        self.customer_id = f"CUST{random.randrange(1,9999):06d}"
        
    def add_product(self) -> dict:
        """ create product details 
        
        Return:
            dict : details of single product
        
        >>> add_product()
        {
            'product_id': 'PROD0042', 
            'product_name': 'Electric Kettle', 
            'product_category': 'Home & Kitchen', 
            'price': 34.99, 'quantity': 2
        }
        """


        product = products[self.product_id]

        return {
            "product_id" : self.product_id,
            "product_name" : product['name'],
            "product_category" : product["category"],
            "price" : product['price'],
            "quantity" : random.randrange(1,4)
        }

    def make_order(self) -> dict:
        """ create sinlge basket with multiple products.

        Retrun:
            dict: basket with the details of the orders.
        
            
        >>> make_order()
        {
            'order_id': '278e004d-98ae-4b76-8a4d-f5b6aa2d9d90', 
            'customer_id': 'CUST007247', 
            'orders_itemas': [
                    {
                    'product_id': 'PROD0020', 
                    'product_name': 'Memory Card', 
                    'product_category': 'Electronics', 
                    'price': 14.99, 
                    'quantity': 3
                    }, 
                    {......},
                    {......}
        """
        
        
        orders = [
            self.add_product()
            for i in range(random.randint(1, 5))
        ]

        

        basket = {
            "order_id" : str(uuid4()),
            "customer_id": self.customer_id,
            "store_id" : random.choice(stores),
            "orders_itemas" : orders,
            "timestamp" : time.time()
        }


        return basket


if __name__ == "__main__":
    test = GenerateOreders()
    print(test.make_order())















