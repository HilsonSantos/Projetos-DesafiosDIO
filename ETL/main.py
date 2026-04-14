from carregar_dados import ProcessoETL

if __name__ == '__main__':
    carregar_dados = ProcessoETL(
        path_txt="scriptssql/sqlserver/createtable/",
        path_xlsx="dados-excel"
    )
    carregar_dados.criar_tabelas()
    carregar_dados.carregar_tabelas()
    carregar_dados.carregar_clientesmsgs()