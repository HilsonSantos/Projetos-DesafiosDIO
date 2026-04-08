import warnings
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from sqlalchemy.exc import SQLAlchemyError, SAWarning

warnings.filterwarnings("ignore", category=SAWarning)



class SQLServer(object):
    def __init__(self):
        self.server = "localhost\\HOSANTOS"
        self.database = "powerbi"
        self.port = 1433
        self.user = "sa"
        self.password = quote_plus("H1s@ntos1969")

    def connect(self):
        engine = create_engine(
            f"mssql+pyodbc://{self.user}:{self.password}@{self.server}:{self.port}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server"
        )
        return engine

    def createtable(self, query):
        try:
            engine = self.connect()
            with engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
        except SQLAlchemyError as e:
            print("Erro na criação da tabela:", e)

    def select(self, query):
        try:
            engine = self.connect()
            with engine.connect() as conn:
                result = conn.execute(text(query))
                data = result.mappings().all()
                return data

        except SQLAlchemyError as e:
            print("Erro na carga:", e)

    def insert(self, query, datas):
        try:
            engine = self.connect()
            with engine.connect() as conn:
                conn.execute(text(query), datas)
                conn.commit()
        except SQLAlchemyError as e:
            print("Erro na carga:", e)