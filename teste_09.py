import mysql.connector
import psycopg2
import pandas as pd

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
            "user"    : "postgres",
            "password": "admin",
            "host"    : "localhost",
            "database": "postgres",
            "port"    : "5432"
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
    

conectar = Conexao()

conectar.conectar_mysql()
conectar.conectar_postgres()

conn_mysql = conectar.obter_conexao_mysql()
conn_postgres = conectar.obter_conexao_postgres()


tabelas_mysql = ['CRM.Clientes', 'CRM.Leads', 'CRM.Enderecos', 'CRM.Telefones', 'sistema.Telefones', 'CRM.Contratos', 'CRM.ContratosCorban', 'sistema.ContratosCorban', 'CRM.Consultas', 'CRM.Tabelas', 'CRM.Parcelas']
tabelas_postgres = ['corban.agendamentos', 'corban.api', 'corban.averbacoes', 'corban.clientes', 'corban.comissionamentos', 'corban.comissoes', 'corban.contrato', 'corban.datas', 'corban.documentos', 'corban.enderecos', 'corban.observacoes', 'corban.propostas', 'corban.sinalizadores', 'corban.telefones', 'extracoes.base_consolidada', 'extracoes.digisac', 'extracoes.disparos', 'extracoes.relatorios']

dfs_mysql = []
for tabela_mysql in tabelas_mysql:
    query_mysql = f"show columns from {tabela_mysql};"

    df = pd.read_sql(query_mysql, conn_mysql)

    df['tabela'] = tabela_mysql

    dfs_mysql.append(df)

df_mysql = pd.concat(dfs_mysql, ignore_index=True)

dfs_postgres = []
for tabela_postgres in tabelas_postgres:
    query_postgres = f"""select
                            a.attname as "Field",
                            format_type(a.atttypid, a.atttypmod) as "Type",
                            isc.is_nullable as "Null",
                            case 
                                when i.indisprimary then 'PRI'
                                when i.indisunique then 'UNI'
                                else ''
                            end as "Key",
                            isc.column_default as "Default",
                            case 
                                when a.attidentity != '' then 'identity'
                                when pg_get_serial_sequence(a.attrelid::regclass::text, a.attname) is not null then 'auto_increment'
                                else ''
                            end as "Extra"
                        from pg_attribute a
                        left join pg_index i
                            on a.attrelid = i.indrelid and a.attnum = any (i.indkey)
                        left join information_schema."columns" isc
                            on a.attname = isc.column_name
                        where a.attrelid = '{tabela_postgres}'::regclass
                            and a.attnum > 0
                            and not a.attisdropped
                            and table_schema = '{tabela_postgres.split('.')[0]}'
                            and table_name = '{tabela_postgres.split('.')[1]}'
                        order by a.attnum;"""


    df = pd.read_sql_query(query_postgres, conn_postgres)

    df['tabela'] = tabela_postgres

    dfs_postgres.append(df)

df_postgres = pd.concat(dfs_postgres, ignore_index=True)

df_final = pd.concat([df_mysql, df_postgres], ignore_index=True)


df_final.to_csv('C:/workspace/scripts_python/arquivos/CSV/info_tabelas.csv', index=False)