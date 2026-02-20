import numpy as np
import pandas as pd
import altair as alt
import streamlit as st
from datetime import date, datetime
from querys.connect import Conexao
from querys.querys_sql import QuerysSQL
from regras.formatadores import Regras
from regras.tratamentos import Tratamentos


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
            weight: auto;
        ">
            <p style="color: white; font-weight: bold; font-size: 0.7vw">{label}</p>
            <h3 style="color: white; font-size: 1.2vw">{value}</h3>
        </div>
        """,
        unsafe_allow_html=True
    )



# =======================================


##### CARREGAR OS DADOS (1x) #####
conectar = Conexao()

conectar.conectar_postgres_aws()
conectar.conectar_postgres()

conn_postgres_aws = conectar.obter_conexao_postgres_aws()
conn_postgres     = conectar.obter_conexao_postgres()

consulta = QuerysSQL()

digisac, corban, crm = consulta.get_acompanhamento()

df_digisac = pd.read_sql_query(digisac, conn_postgres)
df_corban  = pd.read_sql_query(corban, conn_postgres)
df_crm     = pd.read_sql_query(crm, conn_postgres_aws)

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
# ==================== TRATAMENTO DATAS MAIS PROVÁVEIS =====================
# ==========================================================================

mask_corban = df["dt_message"].gt(df["dt_inclusao_corban"])
mask_crm    = df["dt_message"].gt(df["dt_inclusao_crm"])

# ---- CORBAN ----
cols_corban_nat = ["dt_inclusao_corban", "dt_pagamento_corban"]
cols_corban_none = ["valor_liberado_corban", "status_corban", "substatus_corban"]

df.loc[mask_corban, cols_corban_nat] = pd.NaT
df.loc[mask_corban, cols_corban_none] = None

# ---- CRM ----
cols_crm_nat = ["dt_inclusao_crm", "dt_pagamento_crm"]
cols_crm_none = ["valor_liberado_crm", "status_crm", "substatus_crm"]

df.loc[mask_crm, cols_crm_nat] = pd.NaT
df.loc[mask_crm, cols_crm_none] = None

df = df.drop_duplicates()

# ==========================================================================
# ==========================================================================

dados_filtrados = df.copy()

##### ÁREA DO DASHBOARD #####

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE INTERVALO DATA/HORA #####
    df['dt_message'] = (
        pd.to_datetime(df['dt_message'], utc=True)
        .dt.tz_localize(None)
    )

    df['dt_inclusao_corban'] = (
        pd.to_datetime(df['dt_inclusao_corban'], utc=True)
        .dt.tz_localize(None)
    )
    df['dt_inclusao_crm'] = (
        pd.to_datetime(df['dt_inclusao_crm'], utc=True)
        .dt.tz_localize(None)
    )

    df['dt_pagamento_corban'] = (
        pd.to_datetime(df['dt_pagamento_corban'], utc=True)
        .dt.tz_localize(None)
    )
    df['dt_pagamento_crm'] = (
        pd.to_datetime(df['dt_pagamento_crm'], utc=True)
        .dt.tz_localize(None)
    )

    # Inicializa session_state como None
    if "filtro_dt_inicio_mensagem" not in st.session_state:
        st.session_state.filtro_dt_inicio_mensagem = None

    if "filtro_dt_fim_mensagem" not in st.session_state:
        st.session_state.filtro_dt_fim_mensagem = None

    with st.container():
        st.write("Intervalo de Data")

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

                mask = (
                    # dt_message sempre deve estar no intervalo
                    (dados_filtrados['dt_message'] >= dt_inicio) &
                    (dados_filtrados['dt_message'] <= dt_fim)
                )

                # Inclusão corban
                mask &= (
                    (
                        (dados_filtrados['dt_inclusao_corban'] >= dt_inicio) &
                        (dados_filtrados['dt_inclusao_corban'] <= dt_fim)
                    ) |
                    (dados_filtrados['dt_inclusao_corban'].isna())
                )

                # Inclusão crm
                mask &= (
                    (
                        (dados_filtrados['dt_inclusao_crm'] >= dt_inicio) &
                        (dados_filtrados['dt_inclusao_crm'] <= dt_fim)
                    ) |
                    (dados_filtrados['dt_inclusao_crm'].isna())
                )

                # Pagamento corban
                mask &= (
                    (
                        (dados_filtrados['dt_pagamento_corban'] >= dt_inicio) &
                        (dados_filtrados['dt_pagamento_corban'] <= dt_fim)
                    ) |
                    (dados_filtrados['dt_pagamento_corban'].isna())
                )

                # Pagamento crm
                mask &= (
                    (
                        (dados_filtrados['dt_pagamento_crm'] >= dt_inicio) &
                        (dados_filtrados['dt_pagamento_crm'] <= dt_fim)
                    ) |
                    (dados_filtrados['dt_pagamento_crm'].isna())
                )

                dados_filtrados = dados_filtrados[mask]

            else:
                st.warning("A data inicial não pode ser maior que a data final.")

    # Botão de limpeza
    if st.button("🧹 Limpar filtros"):
        for key in list(st.session_state.keys()):
            if key.startswith("filtro_"):
                del st.session_state[key]
        st.rerun()

leads = dados_filtrados.drop_duplicates(subset=['telefone']).copy()
total_leads = (
    len(leads["telefone"])
)

df_departamento = dados_filtrados[['telefone', 'departamento']]
df_departamento = df_departamento.drop_duplicates()
total_departamento = (
    df_departamento.groupby(['departamento']).count()
).reset_index()

digitado_corban = dados_filtrados[['telefone','valor_liberado_corban']]
digitado_corban = digitado_corban.drop_duplicates()
corban_digitado = (
    digitado_corban['valor_liberado_corban']
    .astype(float)
    .sum()
)

df_corban_pago = dados_filtrados[dados_filtrados['dt_pagamento_corban'].notna()]
df_corban_pago = df_corban_pago.drop_duplicates(subset=['telefone', 'valor_liberado_corban'])
corban_pago = (
    df_corban_pago['valor_liberado_corban']
    .astype(float)
    .sum()
)

digitado_crm = dados_filtrados[['telefone', 'valor_liberado_crm']]
digitado_crm = digitado_crm.drop_duplicates()
crm_digitado = (
    digitado_crm['valor_liberado_crm']
    .astype(float)
    .sum()
)

df_crm_pago = dados_filtrados[dados_filtrados['dt_pagamento_crm'].notna()]
df_crm_pago = df_crm_pago.drop_duplicates(subset=['telefone', 'valor_liberado_crm'])
crm_pago = (
    df_crm_pago['valor_liberado_crm']
    .astype(float)
    .sum()
)

total_digitado = corban_digitado + crm_digitado

total_pago = corban_pago + crm_pago

departamento_01 = total_departamento[total_departamento['departamento'] == 'Finalização']
departamento_02 = total_departamento[total_departamento['departamento'] == 'Recepção']
departamento_03 = total_departamento[total_departamento['departamento'] == 'Formalização']
departamento_04 = total_departamento[total_departamento['departamento'] == 'FGTS']
departamento_05 = total_departamento[total_departamento['departamento'] == 'Auditoria/Qualidade']
departamento_06 = total_departamento[total_departamento['departamento'] == 'Chatbot_CLT']
departamento_07 = total_departamento[total_departamento['departamento'] == 'Chatbot_FGTS']
departamento_08 = total_departamento[total_departamento['departamento'] == 'Falcons']
departamento_09 = total_departamento[total_departamento['departamento'] == 'Tigers']

# ==========================================================================
# ======================== TRATAMENTO DOS GRÁFICOS =========================
# ==========================================================================

# df_grafico = total_departamento.copy()
mask_corban = dados_filtrados['dt_pagamento_corban'].notna()
mask_crm    = dados_filtrados['dt_pagamento_crm'].notna()

df_grafico = (
    dados_filtrados
    .groupby('departamento', as_index=False)
    .agg(
        total_digitado_corban=('valor_liberado_corban', 'sum'),
        total_digitado_crm=('valor_liberado_crm', 'sum'),
        total_pago_corban=(
            'valor_liberado_corban',
            lambda x: x[mask_corban.loc[x.index]].sum()
        ),
        total_pago_crm=(
            'valor_liberado_crm',
            lambda x: x[mask_crm.loc[x.index]].sum()
        ),
        total_leads=('telefone', 'count'),
        total_leads_digitado_corban=('dt_inclusao_corban', 'count'),
        total_leads_digitado_crm=('dt_inclusao_crm', 'count'),
        total_leads_pagos_corban=('dt_pagamento_corban', 'count'),
        total_leads_pagos_crm=('dt_pagamento_crm', 'count')
    )
)

# Nova coluna com soma dos digitados
df_grafico['total_digitado'] = (
    df_grafico['total_digitado_corban'] +
    df_grafico['total_digitado_crm']
)

# Nova coluna com soma dos pagos
df_grafico['total_pago'] = (
    df_grafico['total_pago_corban'] +
    df_grafico['total_pago_crm']
)

df_grafico = df_grafico.rename(columns={'telefone': 'contagem'})

# df_grafico['contagem'] = df_grafico['contagem'].astype(int)

# st.bar_chart(df_grafico, x="departamento", y="contagem")

departamento_select = alt.selection_point(
    fields=["departamento"],
    empty="all"
)

graf_departamento = (
    alt.Chart(df_grafico)
    .mark_bar()
    .encode(
        y=alt.Y(
            "departamento:N",
            sort="-x",
            title="Departamentos"
        ),
        x=alt.X(
            "total_leads:Q",
            title="Total de Leads"
        ),
        color=alt.condition(
            departamento_select,
            alt.value("#1f77b4"),
            alt.value("#d3d3d3")
        ),
        tooltip=[
            alt.Tooltip("total_leads:Q", title="Total de Leads"),
            alt.Tooltip("total_leads_digitado_corban:Q", title="Leads Digitados CORBAN"),
            alt.Tooltip("total_leads_pagos_corban:Q", title="Leads Pagos CORBAN"),
            alt.Tooltip("total_leads_digitado_crm:Q", title="Leads Digitados CRM"),
            alt.Tooltip("total_leads_pagos_crm:Q", title="Leads Pagos CRM"),
        ]
    )
    .add_params(departamento_select)
    .properties(
        height=400,
        title="Total de Leads"
    )
)

graf_departamento_02 = (
    alt.Chart(df_grafico)
    .mark_bar()
    .encode(
        y=alt.Y(
            "departamento:N",
            sort="-x",
            title="Departamentos"
        ),
        x=alt.X(
            "total_digitado:Q",
            title="Total de Digitado"
        ),
        color=alt.condition(
            departamento_select,
            alt.value("#1f77b4"),
            alt.value("#d3d3d3")
        ),
        tooltip=[
            alt.Tooltip("total_leads:Q", title="Total de Leads"),
            alt.Tooltip("total_leads_digitado_corban:Q", title="Leads Digitados CORBAN"),
            alt.Tooltip("total_digitado_corban:Q", format=",.2f", title="Valor Digitado CORBAN"),
            alt.Tooltip("total_leads_digitado_crm:Q", title="Leads Digitados CRM"),
            alt.Tooltip("total_digitado_crm:Q", format=",.2f", title="Valor Digitado CRM")
        ]
    )
    .add_params(departamento_select)
    .properties(
        height=400,
        title="Total Digitado"
    )
)

graf_departamento_03 = (
    alt.Chart(df_grafico)
    .mark_bar()
    .encode(
        y=alt.Y(
            "departamento:N",
            sort="-x",
            title="Departamentos"
        ),
        x=alt.X(
            "total_pago:Q",
            title="Total de Pagos"
        ),
        color=alt.condition(
            departamento_select,
            alt.value("#1f77b4"),
            alt.value("#d3d3d3")
        ),
        tooltip=[
            alt.Tooltip("total_leads:Q", title="Total de Leads"),
            alt.Tooltip("total_leads_pagos_corban:Q", title="Leads Pagos CORBAN"),
            alt.Tooltip("total_pago_corban:Q", format=",.2f", title="Valor Pago CORBAN"),
            alt.Tooltip("total_leads_pagos_crm:Q", title="Leads Pagos CRM"),
            alt.Tooltip("total_pago_crm:Q", format=",.2f", title="Valor Pago CRM")
        ]
    )
    .add_params(departamento_select)
    .properties(
        height=400,
        title="Total Pago"
    )
)


# ==========================================================================
# ==========================================================================

##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1a, col_2a = st.columns((2, 8))

    with col_1a:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2a:
        st.title(":blue[Acompanhamento]")

##### ÁREA DOS CARDS #####
with st.container():
    st.subheader(":blue[Quantidade de Leads]")
    # col_1, col_2, col_3, col_4, col_5 = st.columns(5)
    col_1, col_2, col_3, col_4, col_5, col_6, col_7, col_8, col_9, col_10 = st.columns(10)

    # with col_1:
    #     metric_card("Total Leads", f"{total_leads}")
    #     metric_card("Auditoria/Qualidade", f"{departamento_05['telefone'].sum()}")
        
    # with col_2:
    #     metric_card("Finalização", f"{departamento_01['telefone'].sum()}")
    #     metric_card("Chatbot_CLT", f"{departamento_06['telefone'].sum()}")

    # with col_3:
    #     metric_card("Recepção", f"{departamento_02['telefone'].sum()}")
    #     metric_card("Chatbot_FGTS", f"{departamento_07['telefone'].sum()}")

    # with col_4:
    #     metric_card("Formalização", f"{departamento_03['telefone'].sum()}")
    #     metric_card("Falcons", f"{departamento_08['telefone'].sum()}")

    # with col_5:
    #     metric_card("FGTS", f"{departamento_04['telefone'].sum()}")
    #     metric_card("Tigers", f"{departamento_09['telefone'].sum()}")
    with col_1:
        metric_card("Total Leads", f"{total_leads}")
    with col_2:
        metric_card("Auditoria/Qualidade", f"{departamento_05['telefone'].sum()}")
    with col_3:
        metric_card("Finalização", f"{departamento_01['telefone'].sum()}")
    with col_4:
        metric_card("Chatbot_CLT", f"{departamento_06['telefone'].sum()}")
    with col_5:
        metric_card("Recepção", f"{departamento_02['telefone'].sum()}")
    with col_6:
        metric_card("Chatbot_FGTS", f"{departamento_07['telefone'].sum()}")
    with col_7:
        metric_card("Formalização", f"{departamento_03['telefone'].sum()}")
    with col_8:
        metric_card("Falcons", f"{departamento_08['telefone'].sum()}")
    with col_9:
        metric_card("FGTS", f"{departamento_04['telefone'].sum()}")
    with col_10:
        metric_card("Tigers", f"{departamento_09['telefone'].sum()}")
    
with st.container():
    st.subheader(":blue[Valores negociados]")
    # col_1, col_2, col_3 = st.columns(3)
    col_1, col_2, col_3, col_4, col_5, col_6 = st.columns(6)

    # with col_1:
    #     metric_card("CORBAN digitado", f"R$ {corban_digitado:,.2f}".replace('.','|').replace(',','.').replace('|',','))
    #     metric_card("CORBAN pago", f"R$ {corban_pago:,.2f}".replace('.','|').replace(',','.').replace('|',','))

    # with col_2:
    #     metric_card("CONSIG digitado", f"R$ {crm_digitado:,.2f}".replace('.','|').replace(',','.').replace('|',','))
    #     metric_card("CONSIG pago", f"R$ {crm_pago:,.2f}".replace('.','|').replace(',','.').replace('|',','))

    # with col_3:
    #     metric_card("Total digitado", f"R$ {total_digitado:,.2f}".replace('.','|').replace(',','.').replace('|',','))
    #     metric_card("Total pago", f"R$ {total_pago:,.2f}".replace('.','|').replace(',','.').replace('|',','))
    with col_1:
        metric_card("CORBAN digitado", f"R$ {corban_digitado:,.2f}".replace('.','|').replace(',','.').replace('|',','))
    with col_2:
        metric_card("CORBAN pago", f"R$ {corban_pago:,.2f}".replace('.','|').replace(',','.').replace('|',','))
    with col_3:
        metric_card("CONSIG digitado", f"R$ {crm_digitado:,.2f}".replace('.','|').replace(',','.').replace('|',','))
    with col_4:
        metric_card("CONSIG pago", f"R$ {crm_pago:,.2f}".replace('.','|').replace(',','.').replace('|',','))
    with col_5:
        metric_card("Total digitado", f"R$ {total_digitado:,.2f}".replace('.','|').replace(',','.').replace('|',','))
    with col_6:
        metric_card("Total pago", f"R$ {total_pago:,.2f}".replace('.','|').replace(',','.').replace('|',','))

with st.container():
    st.subheader(":blue[Quantidade de Leads x Departamentos]")
    st.altair_chart(graf_departamento, use_container_width=True)

with st.container():
    st.subheader(":blue[Total Valor Digitado x Departamentos]")
    st.altair_chart(graf_departamento_02, use_container_width=True)

with st.container():
    st.subheader(":blue[Total Valor Pago x Departamentos]")
    st.altair_chart(graf_departamento_03, use_container_width=True)
