import numpy as np
import pandas as pd
import altair as alt
import streamlit as st
from datetime import date
# from querys.connect import Conexao
from querys.querys_sql import QuerysSQL
from regras.formatadores import Regras
from regras.tratamentos import Tratamentos

from conexoes.database import Conexao

import warnings
warnings.filterwarnings("ignore")


if not st.session_state.get("authenticated", False):
    st.stop()

if "authenticated" not in st.session_state:
    st.error("Acesso negado. Faça login.")
    st.stop()


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Campanhas Publicitárias",
    page_icon="image/logo_agnus.ico",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

# =============== FUNÇÕES ===============
regras      = Regras()
tratamentos = Tratamentos()


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
            <p style="color: white; font-weight: bold; font-size: 1vw">{label}</p>
            <h3 style="color: white; font-size: 1.5vw">{value}</h3>
        </div>
        """,
        unsafe_allow_html=True
    )



# =======================================


##### CARREGAR OS DADOS (1x) #####
conectar = Conexao('streamlit')

conectar.conectar_postgres_aws()
conectar.conectar_postgres()

conn_postgres_aws = conectar.obter_conexao_postgres_aws()
conn_postgres     = conectar.obter_conexao_postgres()

consulta = QuerysSQL()

digisac, corban, crm = consulta.get_acompanhamento()

df_digisac = pd.read_sql(digisac, conn_postgres)
df_corban  = pd.read_sql(corban, conn_postgres)
df_crm     = pd.read_sql(crm, conn_postgres_aws)

# ============= TRATAMENTOS =============
df_01 = pd.concat([df_corban, df_crm], ignore_index=True)
df = pd.merge(df_01, df_digisac, on='telefone', how='left')

df



##### ÁREA DO DASHBOARD #####

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')




##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1a, col_2a = st.columns((2, 8))

    with col_1a:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2a:
        st.title(":blue[Acompanhamento]")