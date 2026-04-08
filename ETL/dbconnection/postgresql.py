from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from sqlalchemy.exc import SQLAlchemyError


class PostgreSQL(object):
    def __init__(self):
        self.server = "localhost"
        self.database = "postgres"
        self.port = 5432
        self.user = "postgres"
        self.password = quote_plus("H1s@ntos1969")

    def connect(self):
        engine = create_engine(
            f"postgresql://{self.user}:{self.password}@{self.server}:{self.port}/{self.database}"
        )
        return engine

    def createtable(self, query):
        try:
            engine = self.connect()
            with engine.begin() as conn:
                conn.execute(text(query))
        except SQLAlchemyError as e:
            print("Erro na criação da tabela:", e)

    def select(self, query):
        try:
            engine = self.connect()
            with engine.begin() as conn:
                conn.execute(text(query))
        except SQLAlchemyError as e:
            print("Erro na carga:", e)

    def insert(self, query, datas):
        try:
            engine = self.connect()
            with engine.begin() as conn:
                conn.execute(text(query), datas)
        except SQLAlchemyError as e:
            print("Erro na carga:", e)