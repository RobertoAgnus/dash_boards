import mysql.connector
import psycopg2
import streamlit as st

class Conexao:
    def __init__(self):
        self.config_mysql_aws = {
            'user'    : st.secrets["database"]["USER_MYSQL_AWS"],
            'password': st.secrets["database"]["PASS_MYSQL_AWS"],
            'host'    : st.secrets["database"]["HOST_MYSQL_AWS"],
            'database': st.secrets["database"]["DATABASE_MYSQL_AWS"],
            'port'    : st.secrets["database"]["PORT_MYSQL_AWS"]
        }
        self.config_postgres_aws = {
            'user'    : st.secrets["database"]["USER_POSTGRES_AWS"],
            'password': st.secrets["database"]["PASS_POSTGRES_AWS"],
            'host'    : st.secrets["database"]["HOST_POSTGRES_AWS"],
            'database': st.secrets["database"]["DATABASE_POSTGRES_AWS"],
            'port'    : st.secrets["database"]["PORT_POSTGRES_AWS"]
        }
        self.config_postgres = {
            "user"    : st.secrets["database"]["USER_POSTGRES"],
            "password": st.secrets["database"]["PASS_POSTGRES"],
            "host"    : st.secrets["database"]["HOST_POSTGRES"],
            "database": st.secrets["database"]["DATABASE_POSTGRES"],
            "port"    : st.secrets["database"]["PORT_POSTGRES"]
        }
        self.conn_mysql_aws    = None
        self.conn_postgres_aws = None
        self.conn_postgres     = None

    def conectar_mysql_aws(self):
        try:
            self.conn_mysql_aws = mysql.connector.connect(**self.config_mysql_aws)
            print("Conexão MySQL AWS bem-sucedida!")
        except mysql.connector.Error as err:
            print(f"Erro ao conectar ao MySQL AWS: {err}")
            self.conn_mysql_aws = None

    def conectar_postgres_aws(self):
        try:
            self.conn_postgres_aws = psycopg2.connect(**self.config_postgres_aws)
            print("Conexão PostgreSQL AWS bem-sucedida!")
        except psycopg2.Error as err:
            print(f"Erro ao conectar ao PostgreSQL AWS: {err}")
            self.conn_postgres_aws = None

    def conectar_postgres(self):
        try:
            self.conn_postgres = psycopg2.connect(**self.config_postgres)
            # self.conn_postgres.set_client_encoding('WIN1252')
            print("Conexão PostgreSQL bem-sucedida!")
        except psycopg2.Error as err:
            print(f"Erro ao conectar ao PostgreSQL: {err}")
            self.conn_postgres = None

    def desconectar_mysql_aws(self):
        if self.conn_mysql_aws:
            self.conn_mysql_aws.close()
            print("Conexão ao MySQL AWS encerrada.")

    def desconectar_postgres_aws(self):
        if self.conn_postgres_aws:
            self.conn_postgres_aws.close()
            print("Conexão ao PostgreSQL AWS encerrada.")

    def desconectar_postgres(self):
        if self.conn_postgres:
            self.conn_postgres.close()
            self.conectar_postgres = None
            print("Conexão ao PostgreSQL encerrada.")

    def obter_conexao_mysql_aws(self):
        return self.conn_mysql_aws
    
    def obter_conexao_postgres_aws(self):
        return self.conn_postgres_aws
    
    def obter_conexao_postgres(self):
        return self.conn_postgres