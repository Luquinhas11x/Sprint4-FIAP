import cx_Oracle
import os
from dotenv import load_dotenv
import datetime

load_dotenv()

# Configurações de conexão
username = os.getenv("ORACLE_USERNAME") # Seu nome de usuário
password = os.getenv("ORACLE_PASSWORD")    # Sua senha
host = "oracle.fiap.com.br"  # Seu host
port = "1521"  # Porta padrão do Oracle
sid = "orcl"  # SID do banco de dados

# Formata o DSN corretamente
dsn = cx_Oracle.makedsn(host, port, sid=sid)

try:
    # Estabelecer a conexão
    connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
    print("Conectado ao banco de dados Oracle com sucesso!")

    # Criar um cursor
    cursor = connection.cursor()

    # Executar a consulta para coletar dados da tabela DADOS_CACHE_API_EXTERNA
    query = "SELECT * FROM RM95633.DADOS_CACHE_API_EXTERNA"
    cursor.execute(query)

    # Recuperar os dados
    rows = cursor.fetchall()

    # Imprimir os dados de forma formatada
    for row in rows:
        print("---------------------------------------------------")
        print(f"ID: {row[0]}")
        print(f"Mensagem: {row[1]}")
        print(f"Data Inicial: {row[2].strftime('%Y-%m-%d %H:%M:%S') if isinstance(row[2], datetime.datetime) else row[2]}")
        print(f"Data Final: {row[3].strftime('%Y-%m-%d %H:%M:%S') if isinstance(row[3], datetime.datetime) else row[3]}")
        print(f"Status: {row[4]}")
        print(f"URL: {row[5]}")
        print("---------------------------------------------------")

except cx_Oracle.DatabaseError as e:
    print(f"Erro ao conectar ao banco de dados: {e}")

finally:
    # Fechar a conexão e o cursor, se estabelecidos
    if 'cursor' in locals():
        cursor.close()
    if 'connection' in locals() and connection:
        connection.close()
        print("Conexão com o banco de dados foi fechada.")

