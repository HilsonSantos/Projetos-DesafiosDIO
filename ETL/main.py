from carregar_dados import CarregarDados

if __name__ == '__main__':
    carregar_dados = CarregarDados(
        path_txt="scriptssql/sqlserver/createtable/",
        path_xlsx="dados-excel"
    )
    # carregar_dados.criar_tabelas()
    # carregar_dados.carregar_tabelas()
    carregar_dados.carregar_clientesmsgs()