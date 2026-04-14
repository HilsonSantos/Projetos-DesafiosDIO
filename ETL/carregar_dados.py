import warnings
import os
import pandas as pd
from sqlalchemy.exc import SAWarning
from google import genai
from dbconnection.sqlserver import SQLServer

warnings.filterwarnings("ignore", category=SAWarning)

# GROQ_API_KEY = "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"
GOOGLE_API_KEY = "AIzaSyBpmCfoJAkA5dWQ8LMutXh6bOxfhTqpfLE"

class ProcessoETL(object):
    def __init__(self, path_txt, path_xlsx):
        self.path_txt = path_txt
        self.path_xlsx = path_xlsx
        self.db = SQLServer()

    def criar_tabelas(self):
        """
        Essa função tem como objetivo automatizar a criação de tabelas no banco de dados a partir de arquivos de
        script SQL armazenados em um diretório.
        :return:
        """
        # PEGA OS ARQUIVOS *.TXT
        files_txt = os.listdir(self.path_txt)

        # Percorre a lista com todos os arquivos em txt e cria a tabela no banco
        for file in files_txt:
            # Pega cada arquivo do diretório para ler o conteúdo
            with open(self.path_txt+file, "r", encoding="utf-8") as f:
                query = f.read()

            # Executa a função para criação das tabelas no banco de dados
            db = SQLServer()
            db.createtable(query)

    def carregar_tabelas(self):
        db = SQLServer()
        files_xlsx = os.listdir(self.path_xlsx)
        table_name = None

        # Percorre a lista com todos os arquivos excel com os dados para ser inserido no banco
        for file in files_xlsx:
            # Verifica se o aquivo para extração de dados se encontra, se existir entra no bloco
            if file.startswith("base_powerbi_profissional"):
                sheets = pd.ExcelFile(f"dados-excel/{file}")
                list_sheets = sheets.sheet_names

                for sheet in list_sheets:
                    df = pd.read_excel(os.path.join(self.path_xlsx, file), sheet_name=sheet)
                    df.columns = df.columns.str.upper()
                    for column in df.select_dtypes(include=['object', 'string']):
                        df[column] = df[column].str.strip().str.upper()
                        if column == "DATA":
                            df["DATA"] = pd.to_datetime(df["DATA"]).dt.date

                    colunas = df.columns.tolist()
                    sql_columns = ", ".join(colunas)
                    sql_parameters = ", ".join([f":{col}" for col in colunas])

                    # Transformando um DataFrames em Dicionário
                    df_json = df.to_dict(orient="records")

                    match sheet:
                        case "Clientes":
                            table_name = "CARGA_CLIENTES"
                        case "Produtos":
                            table_name = "CARGA_PRODUTOS"
                        case "Vendedores":
                            table_name = "CARGA_VENDEDORES"
                        case "Metas":
                            table_name = "CARGA_METAS"
                        case "Vendas":
                            table_name = "CARGA_VENDAS"

                    # CONSULTAS DAS COLUNAS CHAVES
                    query = f"""
                            SELECT
                                A.NAME AS TABLE_NAME, 
                                STRING_AGG(D.NAME, ',') AS PRIMARY_KEY
                            FROM SYS.TABLES A
                            JOIN SYS.INDEXES B ON B.OBJECT_ID = A.OBJECT_ID AND B.IS_PRIMARY_KEY = 1
                            JOIN SYS.INDEX_COLUMNS C ON C.OBJECT_ID = B.OBJECT_ID AND C.INDEX_ID = B.INDEX_ID
                            JOIN SYS.COLUMNS D ON D.OBJECT_ID = C.OBJECT_ID AND D.COLUMN_ID = C.COLUMN_ID
                            WHERE 1=1
                            AND A.NAME = '{table_name}'
                            GROUP BY A.NAME
                            """
                    rows = db.select(query)
                    campo_filtro = rows[0]["PRIMARY_KEY"].split(',')

                    for _json in df_json:
                        dados = _json

                        if len(campo_filtro) == 1:
                            and_query = f"AND {campo_filtro[0]} = {dados[f"{campo_filtro[0]}"]}"
                        elif len(campo_filtro) == 2:
                            and_query = (f"AND {campo_filtro[0]} = {dados[f"{campo_filtro[0]}"]} "
                                         f"AND {campo_filtro[1]} = {dados[f"{campo_filtro[1]}"]}")
                        else:
                            and_query = (f"AND {campo_filtro[0]} = {dados[f"{campo_filtro[0]}"]} "
                                         f"AND {campo_filtro[1]} = {dados[f"{campo_filtro[1]}"]} "
                                         f"AND {campo_filtro[2]} = {dados[f"{campo_filtro[2]}"]}")

                        query = f"""
                                SELECT COUNT(*) QUANTIDADE
                                FROM {table_name}
                                WHERE 1=1
                                {and_query}
                                """
                        rows = db.select(query)
                        quantidade = rows[0]["QUANTIDADE"]
                        if quantidade == 0:
                            print(dados)
                            query = f"INSERT INTO DBO.{table_name} ({sql_columns}) VALUES ({sql_parameters})"
                            db.insert(query, dados)

    def carregar_clientesmsgs(self):# Query
        query = """
                SELECT
	                DISTINCT
	                ID_VENDEDOR
                FROM CARGA_VENDAS
                WHERE 1=1
                AND DATA BETWEEN '01/04/2026' AND '30/04/2026'
                """
        dados = self.db.select(query)
        for registro in dados:
            id = registro["ID_VENDEDOR"]
            query = f"""
                    WITH ANALISE AS (
                        SELECT
                            DISTINCT
                            A.ID_VENDEDOR,
                            SUM(B.VALOR_META) OVER (PARTITION BY A.ID_VENDEDOR) METAS,
                            SUM(C.VENDAS)  OVER (PARTITION BY A.ID_VENDEDOR) VENDAS
                        FROM CARGA_VENDEDORES A
                        JOIN CARGA_METAS B ON B.ID_VENDEDOR = A.ID_VENDEDOR AND B.ANO = 2026 AND MES = 4
                        JOIN (
                            SELECT
                                DISTINCT
                                ID_VENDEDOR,
                                SUM(VALOR_BRUTO - VALOR_DESCONTO) OVER (PARTITION BY ID_VENDEDOR) VENDAS
                            FROM CARGA_VENDAS
                            WHERE 1=1
                            AND DATA BETWEEN '01/04/2026' AND '30/04/2026'
                        ) C ON C.ID_VENDEDOR = B.ID_VENDEDOR
                    )
                    SELECT *
                    FROM ANALISE
                    WHERE 1=1
                    AND ID_VENDEDOR = {id}
                    """
            dados = self.db.select(query)
            vendedor_id = dados[0]["ID_VENDEDOR"]
            vlr_meta = dados[0]["METAS"]
            vlr_venda = dados[0]["VENDAS"]
            atingimento = round((vlr_venda / vlr_meta) * 100, 2)
            print(f"Vendedor {vendedor_id} teve atingimento de {atingimento}%")
        client = genai.Client(api_key=GOOGLE_API_KEY)
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            # model="gemini-3.1-flash-lite-preview",
            contents="Em poucas palavras, o resultado do atingimento de cada cliente entre as vendas e meta, qual seria a análise?",
        )
        print(response.text)

