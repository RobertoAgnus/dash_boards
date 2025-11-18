import streamlit as st

from datetime import date


##### FUNÇÃO PARA OBTER AS DATAS CONSULTA #####
def get_datas_consulta(dados):
    # Remove linhas com Data Consulta vazia
    dados = dados.dropna(subset=['Data Consulta'])

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = dados['Data Consulta'].min()
    maior_data = date.today()
    
    return menor_data, maior_data

##### FUNÇÃO PARA OBTER AS DATAS DISPAROS #####
def get_datas_disparos(dados):
    # Remove linhas com Data disparos vazia
    dados = dados.dropna(subset=['Data Disparo'])

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = dados['Data Disparo'].min()
    maior_data = date.today()

    return menor_data, maior_data

##### FUNÇÃO PARA OBTER AS DATAS CORBAN #####
def get_datas_corban(dados):
    # Remove linhas com Data Corban vazia
    dados = dados.dropna(subset=['Data Corban'])

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = dados['Data Corban'].min()
    maior_data = date.today()

    return menor_data, maior_data

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
            <h3 style="color: white; font-size: calc(1rem + 1vw)">{value}</h3>
        </div>
        """,
        unsafe_allow_html=True
    )
