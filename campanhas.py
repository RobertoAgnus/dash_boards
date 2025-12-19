import streamlit as st
import altair as alt
import pandas as pd
from datetime import date
from io import BytesIO
import io
import zipfile

from querys.connect import Conexao
from querys.querys_sql import QuerysSQL
from regras.formatadores import formatar_cpf, formatar_telefone

from regras.obter_dados import carregar_dados


# if not st.session_state.get("authenticated", False):
#     st.stop()

##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Campanhas Publicitárias",
    page_icon="image/logo_agnus.ico",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

# if "authenticated" not in st.session_state:
#     st.error("Acesso negado. Faça login.")
#     st.stop()

##### CARREGAR OS DADOS (1x) #####
# dados, df_crm, df_digisac, df_corban = carregar_dados()
conectar = Conexao()

# conectar.conectar_postgres_aws()
conectar.conectar_postgres()

# conn_postgres_aws = conectar.obter_conexao_postgres_aws()
conn_postgres = conectar.obter_conexao_postgres()

consulta = QuerysSQL()

digisac, corban = consulta.get_campanhas()

df_digisac = pd.read_sql_query(digisac, conn_postgres)
df_corban = pd.read_sql_query(corban, conn_postgres)

df_digisac = formatar_cpf(df_digisac, 'cpf_digisac')
df_corban  = formatar_cpf(df_corban, 'cpf_corban')

df_digisac = formatar_telefone(df_digisac, 'fone_digisac')
df_corban  = formatar_telefone(df_corban, 'fone_corban')

df = pd.merge(df_digisac, df_corban, left_on='cpf_digisac', right_on='cpf_corban', how='left')
df = df[df['data_status_api'].notnull()]


# df = df.drop_duplicates(subset=['fone_digisac','fone_corban'])

##### ÁREA DO DASHBOARD #####

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

df
teste = len(df)
teste