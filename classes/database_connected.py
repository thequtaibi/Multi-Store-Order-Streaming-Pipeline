from sqlalchemy import URL, create_engine, text

class DatabaseConnected:
    def __init__(self):
        self.url = URL.create(
            drivername="postgresql+psycopg",
            username="khlk4004",
            password="4004",
            host="localhost",
            port=5432,
            database="ecommerce_practice"
        )
        engine = create_engine(self.url)