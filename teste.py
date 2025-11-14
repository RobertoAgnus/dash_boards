import streamlit as st
import altair as alt
import pandas as pd
import numpy as np
from datetime import date, datetime
from io import BytesIO
import io
import zipfile

from querys.querys_sql import QuerysSQL
from querys.connect import Conexao

pd.set_option('display.max_columns', None)


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Clientes Atendidos",
    page_icon="image/logo_agnus.ico",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

# ##### CRIAR INSTÂNCIA ÚNICA #####
consulta = QuerysSQL()

##### CACHE DE CONSULTAS #####
@st.cache_data(show_spinner=False)
def carregar_dados():
    conectar = Conexao()

    conectar.conectar_mysql()
    conectar.conectar_postgres()

    conn_mysql = conectar.obter_conexao_mysql()
    conn_postgres = conectar.obter_conexao_postgres()
    
    consulta_disparo = consulta.disparos()
    df_disparo = pd.read_sql(consulta_disparo, conn_postgres)
    df_disparo['telefone_disparos'] = df_disparo['telefone_disparos'].astype(str).str.replace(r'^(55)(?=\d{11,})', '', regex=True)
    df_disparo['telefone_disparos'] = df_disparo['telefone_disparos'].astype(str).apply(
        lambda x: x[:2] + '9' + x[2:] if len(x) <= 10 and x.isdigit() else x
    )

    consulta_digisac = consulta.get_digisac()
    df_digisac = pd.read_sql(consulta_digisac, conn_postgres)
    df_digisac['telefone_digisac'] = df_digisac['telefone_digisac'].astype(str).str.replace(r'^(55)(?=\d{11,})', '', regex=True)
    df_digisac['telefone_digisac'] = df_digisac['telefone_digisac'].astype(str).apply(
        lambda x: x[:2] + '9' + x[2:] if len(x) <= 10 and x.isdigit() else x
    )
    
    teste_001 = pd.merge(df_disparo, df_digisac, left_on=['cpf_disparos'], right_on=['cpf_digisac'], how='outer')

    teste_001 = teste_001[(teste_001['telefone_disparos'].isna()) & (teste_001['telefone_disparos'].isnull())]

    return teste_001

df_crm = carregar_dados()

st.dataframe(df_crm)