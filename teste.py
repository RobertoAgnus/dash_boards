from dotenv import load_dotenv
from querys import connect, querys_sql
from psycopg2.extras import execute_values
import pandas as pd
import numpy as np
import requests
import os

# Mostra todas as colunas
pd.set_option('display.max_columns', None)


if __name__ == "__main__":
    # Obtendo a conexão com o banco
    conectar = connect.Conexao()
    conectar.conectar_postgres()
    conn = conectar.obter_conexao_postgres()
    cur = conn.cursor()

    # Criando obojeto das querys
    consulta = querys_sql.QuerysSQL()

    url = 'https://bots.agnusconsig.com.br/dados-cadastrais/'

    