📊 ETL com Python + SQL Server

📌 Sobre o Projeto
Este projeto implementa um pipeline de ETL (Extract, Transform, Load) utilizando Python, com o objetivo de automatizar a criação de estruturas e carga de dados em um banco SQL Server.
A aplicação foi desenvolvida com foco em organização, escalabilidade e reaproveitamento de scripts SQL, sendo ideal para cenários de engenharia de dados, BI e automação de cargas.

⚙️ Funcionalidades
📁 Leitura automatizada de arquivos .txt com scripts SQL
🗄️ Criação dinâmica de tabelas no banco de dados
📊 Carga de dados a partir de arquivos Excel (.xlsx)
🔄 Execução em lote (batch processing)
🧱 Estrutura modular para fácil manutenção
🏗️ Arquitetura do Projeto

ETL/
│
├── dbconnection/
│   └── sqlserver.py        # Conexão com banco de dados
│
├── scriptssql/
│   ├── sqlserver/
│   │   └── createtable/    # Scripts SQL para criação de tabelas
│
├── dados-excel/            # Arquivos Excel para carga
│
├── carregar_dados.py       # Lógica do ETL
├── main.py                 # Execução principal
└── README.md

🔄 Fluxo do ETL
📥 Leitura dos arquivos .txt contendo scripts SQL
🗄️ Criação das tabelas no SQL Server
📊 Leitura dos arquivos Excel
📤 Inserção dos dados nas tabelas correspondentes