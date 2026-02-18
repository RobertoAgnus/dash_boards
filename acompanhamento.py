import numpy as np
import pandas as pd
import altair as alt
import streamlit as st
from datetime import date, datetime
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
df_01 = pd.merge(df_digisac, df_corban, on='telefone', how='left')
df = pd.merge(df_01, df_crm, on='telefone', how='left')

# ==========================================================================
# ========================== TRATAMENTO DE DATAS ===========================
# ==========================================================================

df['dt_inclusao_crm'    ] = df['dt_inclusao_crm' ].dt.tz_localize(None)
df['dt_pagamento_crm'   ] = df['dt_pagamento_crm'].dt.tz_localize(None)

df["dt_inclusao_corban" ] = pd.to_datetime(df["dt_inclusao_corban" ], errors="coerce")
df["dt_inclusao_crm"    ] = pd.to_datetime(df["dt_inclusao_crm"    ], errors="coerce")
df["dt_pagamento_corban"] = pd.to_datetime(df["dt_pagamento_corban"], errors="coerce")
df["dt_pagamento_crm"   ] = pd.to_datetime(df["dt_pagamento_crm"   ], errors="coerce")

df['dt_inclusao_corban' ] = df['dt_inclusao_corban' ].dt.to_pydatetime()
df['dt_inclusao_crm'    ] = df['dt_inclusao_crm'    ].dt.to_pydatetime()
df['dt_pagamento_corban'] = df['dt_pagamento_corban'].dt.to_pydatetime()
df['dt_pagamento_crm'   ] = df['dt_pagamento_crm'   ].dt.to_pydatetime()

df['dt_message'         ] = df['dt_message'         ].dt.to_pydatetime()

# ==========================================================================
# ==========================================================================

df

dados_filtrados = df.copy()

##### ÁREA DO DASHBOARD #####

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE INTERVALO DATA/HORA MENSAGEM #####
    df['dt_message'] = (
        pd.to_datetime(df['dt_message'], utc=True)
        .dt.tz_localize(None)
    )

    # Inicializa session_state como None
    if "filtro_dt_inicio_mensagem" not in st.session_state:
        st.session_state.filtro_dt_inicio_mensagem = None

    if "filtro_dt_fim_mensagem" not in st.session_state:
        st.session_state.filtro_dt_fim_mensagem = None

    with st.container():
        st.write("Data da Mensagem")

        col1, col2 = st.columns(2)

        with col1:
            dt_inicio = st.datetime_input(
                "Início:",
                value=st.session_state.filtro_dt_inicio_mensagem,
                key="filtro_dt_inicio_mensagem"
            )

        with col2:
            dt_fim = st.datetime_input(
                "Fim:",
                value=st.session_state.filtro_dt_fim_mensagem,
                key="filtro_dt_fim_mensagem"
            )

        # Aplica filtro somente se ambos forem definidos
        if dt_inicio and dt_fim:

            if dt_inicio <= dt_fim:
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['dt_message'] >= dt_inicio) &
                    (dados_filtrados['dt_message'] <= dt_fim)
                ]
            else:
                st.warning("A data inicial não pode ser maior que a data final.")

    ##### FILTRO DE INTERVALO DATA/HORA INCLUSÃO #####
    df['dt_inclusao_corban'] = (
        pd.to_datetime(df['dt_inclusao_corban'], utc=True)
        .dt.tz_localize(None)
    )
    df['dt_inclusao_crm'] = (
        pd.to_datetime(df['dt_inclusao_crm'], utc=True)
        .dt.tz_localize(None)
    )

    # Inicializa session_state como None
    if "filtro_dt_inicio_inclusao" not in st.session_state:
        st.session_state.filtro_dt_inicio_inclusao = None

    if "filtro_dt_fim_inclusao" not in st.session_state:
        st.session_state.filtro_dt_fim_inclusao = None

    with st.container():
        st.write("Data da Inclusão")

        col1, col2 = st.columns(2)

        with col1:
            dt_inicio = st.datetime_input(
                "Início:",
                value=st.session_state.filtro_dt_inicio_inclusao,
                key="filtro_dt_inicio_inclusao"
            )

        with col2:
            dt_fim = st.datetime_input(
                "Fim:",
                value=st.session_state.filtro_dt_fim_inclusao,
                key="filtro_dt_fim_inclusao"
            )

        # Aplica filtro somente se ambos forem definidos
        if dt_inicio and dt_fim:

            if dt_inicio <= dt_fim:
                dados_filtrados = dados_filtrados[(dados_filtrados['dt_inclusao_corban'].notna()) | (dados_filtrados['dt_inclusao_crm'].notna())]
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['dt_inclusao_corban'] >= dt_inicio) &
                    (dados_filtrados['dt_inclusao_corban'] <= dt_fim) |
                    (dados_filtrados['dt_inclusao_corban'].isna())
                ]
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['dt_inclusao_crm'] >= dt_inicio) &
                    (dados_filtrados['dt_inclusao_crm'] <= dt_fim) |
                    (dados_filtrados['dt_inclusao_crm'].isna())
                ]
            else:
                st.warning("A data inicial não pode ser maior que a data final.")

    ##### FILTRO DE INTERVALO DATA/HORA PAGAMENTO #####
    df['dt_pagamento_corban'] = (
        pd.to_datetime(df['dt_pagamento_corban'], utc=True)
        .dt.tz_localize(None)
    )
    df['dt_pagamento_crm'] = (
        pd.to_datetime(df['dt_pagamento_crm'], utc=True)
        .dt.tz_localize(None)
    )

    # Inicializa session_state como None
    if "filtro_dt_inicio_pagamento" not in st.session_state:
        st.session_state.filtro_dt_inicio_pagamento = None

    if "filtro_dt_fim_pagamento" not in st.session_state:
        st.session_state.filtro_dt_fim_pagamento = None

    with st.container():
        st.write("Data do Pagamento")

        col1, col2 = st.columns(2)

        with col1:
            dt_inicio = st.datetime_input(
                "Início:",
                value=st.session_state.filtro_dt_inicio_pagamento,
                key="filtro_dt_inicio_pagamento"
            )

        with col2:
            dt_fim = st.datetime_input(
                "Fim:",
                value=st.session_state.filtro_dt_fim_pagamento,
                key="filtro_dt_fim_pagamento"
            )

        # Aplica filtro somente se ambos forem definidos
        if dt_inicio and dt_fim:

            if dt_inicio <= dt_fim:
                dados_filtrados = dados_filtrados[(dados_filtrados['dt_pagamento_corban'].notna()) | (dados_filtrados['dt_pagamento_crm'].notna())]
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['dt_pagamento_corban'] >= dt_inicio) &
                    (dados_filtrados['dt_pagamento_corban'] <= dt_fim) |
                    (dados_filtrados['dt_pagamento_corban'].isna())
                ]
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['dt_pagamento_crm'] >= dt_inicio) &
                    (dados_filtrados['dt_pagamento_crm'] <= dt_fim) |
                    (dados_filtrados['dt_pagamento_crm'].isna())
                ]
            else:
                st.warning("A data inicial não pode ser maior que a data final.")

    # Botão de limpeza
    if st.button("🧹 Limpar filtros"):
        for key in list(st.session_state.keys()):
            if key.startswith("filtro_"):
                del st.session_state[key]
        st.rerun()

dados_filtrados

total_leads = (
    len(dados_filtrados["dt_message"])
)

df_departamento = dados_filtrados[['telefone', 'departamento']]
total_finalizacao = (
    df_departamento.groupby(['departamento']).count()
)

corban_digitado = (
    dados_filtrados['valor_liberado_corban']
    .astype(float)
    .sum()
)

df_corban_pago = dados_filtrados[dados_filtrados['dt_pagamento_corban'].notna()]
corban_pago = (
    df_corban_pago['valor_liberado_corban']
    .astype(float)
    .sum()
)

crm_digitado = (
    dados_filtrados['valor_liberado_crm']
    .astype(float)
    .sum()
)

df_crm_pago = dados_filtrados[dados_filtrados['dt_pagamento_crm'].notna()]
crm_pago = (
    df_crm_pago['valor_liberado_crm']
    .astype(float)
    .sum()
)

total_digitado = corban_digitado + crm_digitado

total_pago = corban_pago + crm_pago

total_leads
total_finalizacao
corban_digitado
corban_pago
crm_digitado
crm_pago
total_digitado
total_pago

##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1a, col_2a = st.columns((2, 8))

    with col_1a:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2a:
        st.title(":blue[Acompanhamento]")