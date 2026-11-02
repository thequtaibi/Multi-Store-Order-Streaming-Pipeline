from sqlalchemy import URL, create_engine, text


class DatabaseConnector:
    def __init__(self, drivername, username, password, host, port, database):
        self.drivername = drivername
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def database_connection(self):
        DATABASE_URL = URL.create(
            drivername=self.drivername,
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database
            )
        engine = create_engine(DATABASE_URL)
        return engine