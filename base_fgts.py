import streamlit as st
import pandas as pd
import altair as alt
from datetime import date, datetime

from querys.querys_sql import QuerysSQL
from querys.connect import Conexao

# ## REMOVER QUANDO FOR PARA PRODUÇÃO ##
# from querys.querys_csv import QuerysSQL
# import duckdb as dk


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Base FGTS -> CLT",
    page_icon="🏂",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

##### CONEXÃO COM O BANCO DE DADOS #####
# Criar uma instância da classe Conexao
conectar_mysql = Conexao()
conectar_postgres = Conexao()
conectar_mysql.conectar_mysql()
conectar_postgres.conectar_postgres()

# Conectando ao banco de dados MySQL
conn_mysql = conectar_mysql.obter_conexao_mysql()
conn_postgres = conectar_postgres.obter_conexao_postgres()



##### CRIAR INSTÂNCIA DO BANCO #####
consulta = QuerysSQL()


##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE PERFIL DOS CLIENTES #####
    selectbox_perfil = st.selectbox(
        'Selecione o Perfil do Cliente',
        ["Contratados", "+3 Meses", "Sem Contrato", "Sem CPF"],
        index=0
    )

##### FUNÇÃO PARA GERAR OS CARDS #####
def metric_card(label, value):
    st.markdown(
        f"""
        <div style="
            background-color: #262730;
            border-radius: 10px;
            text-align: center;
            margin-bottom: 15px;
            height: auto;
        ">
            <p style="color: white; font-weight: bold;">{label}</p>
            <h3 style="color: white; font-size: 40px">{value}</h3>
        </div>
        """,
        unsafe_allow_html=True
    )

##### TRATAR NÚMEROS DE TELEFONES #####
def tratar_numero(num):
    if num != None:
        # Garante que é string
        num = str(num)
        
        # Se tem mais de 11 caracteres e começa com "55" → remove os dois primeiros
        if len(num) > 11 and num.startswith("55"):
            num = num[2:]
        
        # Se depois da remoção ficou com menos de 11 caracteres → insere '9' após o 2º caractere
        if len(num) < 11:
            num = num[:2] + '9' + num[2:]
    
    return num

##### OBTEM TELEFONES DA API CORBAN #####
telefones_corban = consulta.obtem_telefones()
df_telefones_corban = pd.read_sql_query(telefones_corban, conn_postgres)
df_telefones_corban["telefoneAPICorban"] = df_telefones_corban["telefoneAPICorban"].apply(tratar_numero)

##### CLIENTES CONTRATADOS #####
condicao = "produto = '4111' AND dataInclusao IS NOT NULL"
clientes_contratados = consulta.consulta_base_fgts(condicao)
df_clientes_contratados = pd.read_sql(clientes_contratados, conn_mysql)

df_clientes_contratados["telefone"] = df_clientes_contratados["telefone"].apply(tratar_numero)
df_clientes_contratados["telefoneLeads"] = df_clientes_contratados["telefoneLeads"].apply(tratar_numero)

df_clientes_contratados['valorFinanciado'] = df_clientes_contratados['valorFinanciado'].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
df_clientes_contratados['valorLiberado'] = df_clientes_contratados['valorLiberado'].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

df_clientes_contratados_telefones = pd.merge(df_clientes_contratados, df_telefones_corban, how='left', on='CPF')

qtd_clientes_contratados = len(df_clientes_contratados_telefones.drop_duplicates(subset='CPF'))

##### CLIENTES CONTRATADOS +3MESES #####
condicao = "produto = '4111' and dataInclusao < '2025-08-01 00:00:00'"
clientes_contratados_mais_3_meses = consulta.consulta_base_fgts(condicao)
df_clientes_contratados_mais_3_meses = pd.read_sql(clientes_contratados_mais_3_meses, conn_mysql)

df_clientes_contratados_mais_3_meses["telefone"] = df_clientes_contratados_mais_3_meses["telefone"].apply(tratar_numero)
df_clientes_contratados_mais_3_meses["telefoneLeads"] = df_clientes_contratados_mais_3_meses["telefoneLeads"].apply(tratar_numero)

df_clientes_contratados_mais_3_meses['valorFinanciado'] = df_clientes_contratados_mais_3_meses['valorFinanciado'].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
df_clientes_contratados_mais_3_meses['valorLiberado'] = df_clientes_contratados_mais_3_meses['valorLiberado'].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

df_clientes_contratados_mais_3_meses_telefones = pd.merge(df_clientes_contratados_mais_3_meses, df_telefones_corban, how='left', on='CPF')

qtd_clientes_contratados_mais_3_meses = len(df_clientes_contratados_mais_3_meses_telefones.drop_duplicates(subset='CPF'))

##### CLIENTES SEM CONTRATOS #####
condicao = "dataInclusao is null"
clientes_sem_contratos = consulta.consulta_base_fgts(condicao)
df_clientes_sem_contratos = pd.read_sql(clientes_sem_contratos, conn_mysql)

df_clientes_sem_contratos["telefone"] = df_clientes_sem_contratos["telefone"].apply(tratar_numero)
df_clientes_sem_contratos["telefoneLeads"] = df_clientes_sem_contratos["telefoneLeads"].apply(tratar_numero)

df_clientes_sem_contratos_telefones = pd.merge(df_clientes_sem_contratos, df_telefones_corban, how='left', on='CPF')

qtd_clientes_sem_contratos = len(df_clientes_sem_contratos_telefones.drop_duplicates(subset='CPF'))

##### CLIENTES SEM CPF #####
clientes_sem_cpf = consulta.clientes_sem_cpf()
df_clientes_sem_cpf = pd.read_sql(clientes_sem_cpf, conn_mysql)
df_clientes_sem_cpf["telefone"] = df_clientes_sem_cpf["telefone"].apply(tratar_numero)

qtd_clientes_sem_cpf = len(df_clientes_sem_cpf.drop_duplicates(subset='telefone'))


df_tabela = pd.DataFrame()
texto_tabela = ""
if selectbox_perfil == "Contratados":
    df_tabela = df_clientes_contratados_telefones
    texto_tabela = "Contratados"
elif selectbox_perfil == "+3 Meses":
    df_tabela = df_clientes_contratados_mais_3_meses_telefones
    texto_tabela = "Contratados +3 Meses"
elif selectbox_perfil == "Sem Contrato":
    df_tabela = df_clientes_sem_contratos_telefones
    texto_tabela = "Sem Contrato"
elif selectbox_perfil == "Sem CPF":
    df_tabela = df_clientes_sem_cpf
    texto_tabela = "Sem CPF"

##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1, col_2 = st.columns((1, 8.5))

    with col_1:
        st.image("image/logo_agnus_v3.jpg", width=200)
    with col_2:
        st.title(":blue[Análise dos clientes atendidos no sistema]")

##### CORPO DO DASHBOARD #####
with st.container():
    col_1, col_2 = st.columns((1.5, 8.5))

    ##### ÁREA DOS CARDS #####
    with col_1:
        st.markdown("### :blue[Perfil dos Clientes]")

        ##### CARD TOTAL CLIENTES CONTRATADOS #####
        metric_card("Total de Clientes Contratados", f"{format(int(qtd_clientes_contratados), ',').replace(',', '.')}")

        ##### CARD CLIENTES CONTRATADOS +3 MESES #####
        metric_card("Clientes Contratados +3 Meses", f"{format(int(qtd_clientes_contratados_mais_3_meses), ',').replace(',', '.')}")

        ##### CARD CLIENTES SEM CONTRATOS #####
        metric_card("Clientes Sem Contratos", f"{format(int(qtd_clientes_sem_contratos), ',').replace(',', '.')}")

        ##### CARD CLIENTES SEM CPF #####
        metric_card("Clientes Sem CPF", f"{format(int(qtd_clientes_sem_cpf), ',').replace(',', '.')}")

    ##### ÁREA DA TABELA #####
    with col_2:
        ##### TABELA DE CLIENTES #####
        st.markdown(f"### :blue[Detalhamento dos Clientes {texto_tabela}]")
        st.dataframe(df_tabela, width='stretch', height=500, hide_index=True)

        ##### BOTÃO EXPORTAR TABELA #####
        csv = df_tabela.to_csv(index=False)
        st.download_button(
            label="⬇️ Baixar planilha",
            data=csv,
            file_name="dados.csv",
            mime="text/csv",
        )


##################################################################################
###################################### TESTE #####################################
##################################################################################

# query_postgres = """select 
#                         concat(tc.ddd, tc.numero) as telefone, 
#                         LPAD(cc.cliente_cpf::TEXT, 11, '0') AS "CPF"
#                     from "propostasCorban".telefones_concatenado tc 
#                     left join "propostasCorban".clientes_concatenado cc on tc.cliente_id = cc.cliente_id;"""

# query_mysql = """select 
#                     c.CPF,  
#                     c.id as clienteId
#                 from CRM.Clientes c
#                 left join CRM.Telefones t on c.id = t.clienteId
#                 where t.telefone is null;"""

# df_postegres = pd.read_sql_query(query_postgres, conn_postgres)
# df_mysql = pd.read_sql(query_mysql, conn_mysql)

# df_final = pd.merge(df_postegres, df_mysql, on="CPF", how="inner")

# df_final[['telefone', 'clienteId']]