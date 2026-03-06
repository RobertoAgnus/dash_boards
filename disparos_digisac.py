import re
import unicodedata
import numpy as np
import pandas as pd
import altair as alt
import streamlit as st
from datetime import date
from querys.connect import Conexao
from querys.querys_sql import QuerysSQL
from querys.gravar_bd import GravarBandoDados
from regras.obter_dados import carregar_dados
from regras.formatadores import Regras
from regras.tratamentos import Tratamentos
from io import BytesIO
import io
import zipfile

# from conexoes.database import Conexao

import warnings
warnings.filterwarnings("ignore")


if not st.session_state.get("authenticated", False):
    st.stop()

if "authenticated" not in st.session_state:
    st.error("Acesso negado. Faça login.")
    st.stop()


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Disapros DIGISAC",
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
df = carregar_dados('disparos_digisac')

# =============== DATAS =================
# Data Atual
data_atual = pd.Timestamp.now()

# Padroniza colunas de datas
df['dt_message'] = (
    pd.to_datetime(df['dt_message'], utc=True)
    .dt.tz_localize(None)
)

df['dt_disparo'] = (
    pd.to_datetime(df['dt_disparo'], utc=True)
    .dt.tz_localize(None)
)

df['name'] = np.where(
    (df['internal_name'].isna()) | 
    (df['internal_name'] == ''), 
    df['name'], 
    df['internal_name']
)

df['qtd_disparos'] = df['qtd_disparos'].astype(str).str.replace('.0', '')

df['qtd_disparos'] = np.where(
    (df['qtd_disparos'].isna()) |
    (df['qtd_disparos'] == 'nan'),
    '0',
    df['qtd_disparos']
)

df = df[(df['label'].notna()) & (df['label'] != 'implementação facta')]

df = df.drop_duplicates()

dados_filtrados = df.copy()

##### ÁREA DO DASHBOARD #####

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE TAGS #####
    consulta_tag = df['label'].dropna().unique().tolist()
    consulta_tag = [str(x).strip() for x in consulta_tag if x is not None]
    consulta_tag = sorted(consulta_tag)
    
    if "filtro_tag" not in st.session_state:
        st.session_state.filtro_tag = []

    selectbox_tag = st.multiselect(
        'Selecione a tag',
        consulta_tag,
        key="filtro_tag",
        placeholder='Selecionar'
    )
    
    if len(selectbox_tag) != 0:
        dados_filtrados['label'] = dados_filtrados['label'].astype(str).str.strip()
        filtros = [str(x).strip() for x in selectbox_tag]
        dados_filtrados = dados_filtrados[dados_filtrados['label'].isin(filtros)]

    ##### FILTRO DE FALHAS #####
    consulta_falha = df['falha'].dropna().unique().tolist()
    consulta_falha = [str(x).strip() for x in consulta_falha if x is not None]
    consulta_falha = sorted(consulta_falha)
    
    if "filtro_falha" not in st.session_state:
        st.session_state.filtro_falha = []

    selectbox_falha = st.multiselect(
        'Selecione a falha',
        consulta_falha,
        key="filtro_falha",
        placeholder='Selecionar'
    )
    
    if len(selectbox_falha) != 0:
        dados_filtrados['falha'] = dados_filtrados['falha'].astype(str).str.strip()
        filtros = [str(x).strip() for x in selectbox_falha]
        dados_filtrados = dados_filtrados[dados_filtrados['falha'].isin(filtros)]

    ##### FILTRO DE DISPAROS #####
    qtd_disparos = df['qtd_disparos'].dropna().unique().tolist()
    qtd_disparos = [str(x).strip() for x in qtd_disparos if x is not None]
    qtd_disparos = sorted(qtd_disparos)
    
    if "filtro_disparos" not in st.session_state:
        st.session_state.filtro_disparos = []

    selectbox_disparos = st.multiselect(
        'Quantidade de Disparos',
        qtd_disparos,
        key="filtro_disparos",
        placeholder='Selecionar'
    )
    
    if len(selectbox_disparos) != 0:
        dados_filtrados['qtd_disparos'] = dados_filtrados['qtd_disparos'].astype(str).str.strip()
        filtros = [str(x).strip() for x in selectbox_disparos]
        dados_filtrados = dados_filtrados[dados_filtrados['qtd_disparos'].isin(filtros)]
        

    ##### FILTRO DE INTERVALO DE DATA TAG #####
    dados_filtrados['dt_message'] = (
        pd.to_datetime(dados_filtrados['dt_message'], errors='coerce', utc=True)
        .dt.tz_localize(None)
        .dt.date
    )

    menor_data_mensagem, maior_data_mensagem = tratamentos.get_datas(df, 'dt_message')
    menor_data_mensagem = pd.to_datetime(menor_data_mensagem).date()

    if "filtro_periodo_mensagem" not in st.session_state:
        st.session_state.filtro_periodo_mensagem = (menor_data_mensagem, date.today())
    
    if not pd.isnull(menor_data_mensagem):
        intervalo_mensagem = st.date_input(
            "Data da TAG:",
            value=(),
            key="filtro_periodo_mensagem"
        )
        
        # Se o usuário selecionou apenas uma data, define fim como hoje
        if len(intervalo_mensagem) == 2:
            inicio_mensagem, fim_mensagem = intervalo_mensagem
            
        elif len(intervalo_mensagem) == 1:
            # Usuário selecionou apenas uma data
            inicio_mensagem = intervalo_mensagem[0]
            fim_mensagem = date.today()
            
        # Se o usuário não alterou o intervalo, mantém todas as linhas (inclusive NaT)
        try:
            
            if (inicio_mensagem, fim_mensagem) != (menor_data_mensagem, maior_data_mensagem):
                # Filtra as linhas de consulta dentro do intervalo
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['dt_message'] >= inicio_mensagem) &
                    (dados_filtrados['dt_message'] <= fim_mensagem)
                ]
                
        except:
            
            dados_filtrados = dados_filtrados[
                dados_filtrados['dt_message'].isna()
            ]

    ##### FILTRO DE INTERVALO DE DATA DISPAROS #####
    dados_filtrados['dt_disparo'] = (
        pd.to_datetime(dados_filtrados['dt_disparo'], errors='coerce', utc=True)
        .dt.tz_localize(None)
        .dt.date
    )
    
    menor_data_disparo, maior_data_disparo = tratamentos.get_datas(df, 'dt_disparo')
    menor_data_disparo = pd.to_datetime(menor_data_disparo).date()

    if "filtro_periodo_disparo" not in st.session_state:
        st.session_state.filtro_periodo_disparo = (menor_data_disparo, date.today())
    
    if not pd.isnull(menor_data_disparo):
        intervalo_disparo = st.date_input(
            "Data do Disparo:",
            value=(),
            key="filtro_periodo_disparo"
        )
        
        # Se o usuário selecionou apenas uma data, define fim como hoje
        if len(intervalo_disparo) == 2:
            inicio_disparo, fim_disparo = intervalo_disparo
            
        elif len(intervalo_disparo) == 1:
            # Usuário selecionou apenas uma data
            inicio_disparo = intervalo_disparo[0]
            fim_disparo = date.today()
            
        # Se o usuário não alterou o intervalo, mantém todas as linhas (inclusive NaT)
        try:
            
            if (inicio_disparo, fim_disparo) != (menor_data_disparo, maior_data_disparo):
                # Filtra as linhas de consulta dentro do intervalo
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['dt_disparo'] >= inicio_disparo) &
                    (dados_filtrados['dt_disparo'] <= fim_disparo)
                ]
                
        except:
            
            dados_filtrados = dados_filtrados[
                dados_filtrados['dt_disparo'].isna()
            ]

    # Botão de limpeza
    if st.button("🧹 Limpar filtros"):
        for key in list(st.session_state.keys()):
            if key.startswith("filtro_"):
                del st.session_state[key]
        st.rerun()


dados_filtrados = dados_filtrados[['name', 'number', 'dt_message', 'label', 'falha', 'qtd_disparos', 'dt_disparo']]

df_exibir = dados_filtrados.copy()

df_exibir = df_exibir.rename(
    columns={
        'name'        : 'Nome'            , 
        'number'      : 'Telefone'        , 
        'dt_message'  : 'Data da Mensagem', 
        'label'       : 'TAG'             , 
        'falha'       : 'Falha'           ,
        'qtd_disparos': 'Disparos'        , 
        'dt_disparo'  : 'Último Disparo'
    }
)

dados_csv = dados_filtrados[['name', 'number']]
dados_csv = dados_csv.rename(columns={'name': 'Nome', 'number': 'Telefone'})
dados_csv = dados_csv.drop_duplicates()
dados_csv['Telefone'] = '55' + dados_csv['Telefone']

##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1a, col_2a = st.columns((2, 8))

    with col_1a:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2a:
        st.title(":blue[Análise DIGISAC]")
    
with st.container():
    st.subheader(":blue[Clientes DIGISAC]")
    st.dataframe(df_exibir, width='stretch', height=500, hide_index=True)
    st.write(f'Quantidade: {len(df_exibir)}')


def gravar_disparos():
    gravar = GravarBandoDados()

    df_disparos_atual = df_exibir[['Telefone', 'Disparos', 'Último Disparo', 'TAG']]

    df_disparos_atual = df_disparos_atual.rename(columns={'Telefone': 'number', 'Disparos': 'qtd_disparos', 'Último Disparo': 'dt_disparo', 'TAG': 'tag'})

    df_disparos_atual['qtd_disparos'] = np.where(
        df_disparos_atual['qtd_disparos'].isna(),
        1,
        df_disparos_atual['qtd_disparos'].astype(int) + 1
    )
    df_disparos_atual['dt_disparo'] = data_atual
    
    # self.gravar_bd.upsert_dataframe(df_disparos_atual)
    # df_disparos_atual
    gravar.upsert_dataframe(df_disparos_atual)
    
with st.container():
    ##### BOTÃO EXPORTAR TABELA #####
    # qtd_total = len(dados_csv)
    # if qtd_total > 0:
    #     visibilidade_total = False
    # else:
    #     visibilidade_total = True

    # buffer = BytesIO()
    # with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
    #     dados_csv.to_excel(writer, index=False, sheet_name='Visualização')
    # buffer.seek(0)  # volta o ponteiro para o início
    

    # # --- Botão para download ---
    # st.write('')
    # st.write('')
    # st.download_button(
    #     label="⬇️ Baixar Lista",
    #     data=buffer,
    #     file_name=f"lista_digisac_{date.today()}.xlsx",
    #     mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #     disabled=visibilidade_total
    # )

    qtd_total = len(dados_csv)
    visibilidade_total = qtd_total == 0

    # # Botão de ação (chama método)
    # if st.button("💾 Gravar dados", disabled=visibilidade_total):
    #     teste()
        
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        dados_csv.to_excel(writer, index=False, sheet_name="Visualização")
    buffer.seek(0)

    if st.download_button(
        label="⬇️ Baixar Lista",
        data=buffer,
        file_name=f"lista_digisac_{date.today()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        disabled=visibilidade_total
    ):
        gravar_disparos()