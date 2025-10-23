import mysql.connector
import psycopg2

class Conexao:
    def __init__(self):
        # Configurações de conexão ao banco de dados MySQL
        self.config_mysql = {
            'user'    : 'Jeferson',
            'password': '58GRY4Kj1oCbdEEfJbha',
            'host'    : 'sistema-crm.cwzfl6uvan6o.us-east-1.rds.amazonaws.com',
            'database': 'sistema',
            'port'    : '3306'
        }
        self.config_postgres = {
            "host":"localhost",
            "port":"5432",
            "database":"postgres",
            "user":"postgres",
            "password":"admin"
        }
        self.conn_mysql    = None
        self.conn_postgres = None

    def conectar_mysql(self):
        try:
            self.conn_mysql = mysql.connector.connect(**self.config_mysql)
            print("Conexão MySQL bem-sucedida!")
        except mysql.connector.Error as err:
            print(f"Erro ao conectar ao MySQL: {err}")
            self.conn_mysql = None

    def conectar_postgres(self):
        try:
            print(self.config_postgres)
            self.conn_postgres = psycopg2.connect(**self.config_postgres)
            # self.conn_postgres.set_client_encoding('WIN1252')
            print("Conexão PostgreSQL bem-sucedida!")
        except psycopg2.Error as err:
            print(f"Erro ao conectar ao PostgreSQL: {err}")
            self.conn_postgres = None

    def desconectar_mysql(self):
        if self.conn_mysql:
            self.conn_mysql.close()
            print("Conexão ao MySQL encerrada.")

    def desconectar_postgres(self):
        if self.conn_postgres:
            self.conn_postgres.close()
            self.conectar_postgres = None
            print("Conexão ao PostgreSQL encerrada.")

    def obter_conexao_mysql(self):
        return self.conn_mysql
    
    def obter_conexao_postgres(self):
        return self.conn_postgres