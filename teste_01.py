from querys.connect import Conexao
import pandas as pd

##### CONEXÃO COM O BANCO DE DADOS #####
# Criar uma instância da classe Conexao
conectar_mysql = Conexao()
conectar_postgres = Conexao()
conectar_mysql.conectar_mysql()
conectar_postgres.conectar_postgres()

# Conectando ao banco de dados MySQL
conn_mysql = conectar_mysql.obter_conexao_mysql()
conn_postgres = conectar_postgres.obter_conexao_postgres()

query_postgres = """select 
                        concat(tc.ddd, tc.numero) as telefone, 
                        LPAD(cc.cliente_cpf::TEXT, 11, '0') AS "CPF"
                    from "propostasCorban".telefones_concatenado tc 
                    left join "propostasCorban".clientes_concatenado cc on tc.cliente_id = cc.cliente_id;"""

query_mysql = """select 
                    c.CPF,  
                    c.id
                from CRM.Clientes c
                left join CRM.Telefones t on c.id = t.clienteId
                where t.telefone is null;"""

df_postegres = pd.read_sql_query(query_postgres, conn_postgres)
df_mysql = pd.read_sql(query_mysql, conn_mysql)

df_final = pd.merge(df_postegres, df_mysql, on="CPF", how="inner")

print(df_final.head())