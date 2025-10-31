import streamlit as st
import altair as alt
import pandas as pd
import itertools
from datetime import date, datetime

from querys.querys_sql import QuerysSQL
from querys.connect import Conexao


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Clientes Atendidos",
    page_icon="image/logo_agnus.ico",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

# ##### CRIAR INSTÂNCIA ÚNICA #####
consulta = QuerysSQL()

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

@st.cache_data
def get_total_clientes_unicos(total_clientes_unicos):
    conectar = Conexao()
    conectar.conectar_mysql()
    conn = conectar.obter_conexao_mysql()
    df = pd.read_sql(total_clientes_unicos, conn)

    conectar.desconectar_mysql()

    return df

@st.cache_data
def get_qtd_clientes_atendidos(qtd_clientes_atendidos):
    conectar = Conexao()
    conectar.conectar_mysql()
    conn = conectar.obter_conexao_mysql()
    df = pd.read_sql(qtd_clientes_atendidos, conn)

    conectar.desconectar_mysql()

    return df

@st.cache_data
def get_etapa_atendimento(etapa_atendimento):
    conectar = Conexao()
    conectar.conectar_mysql()
    conn = conectar.obter_conexao_mysql()
    df = pd.read_sql(etapa_atendimento, conn)

    conectar.desconectar_mysql()

    return df

@st.cache_data
def get_datas(datas):
    conectar = Conexao()
    conectar.conectar_mysql()
    conn = conectar.obter_conexao_mysql()
    df = pd.read_sql(datas, conn)

    conectar.desconectar_mysql()

    return df

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE Etapa #####
    etapa_atendimento = consulta.etapa_atendimentos()
    etapa = get_etapa_atendimento(etapa_atendimento)
    etapa = etapa[etapa['Etapa'].isin(['LEAD_NOVO', 'SEM_SALDO', 'NAO_AUTORIZADO', 'COM_SALDO'])]

    # Adiciona selectbox etapa na sidebar:
    selectbox_etapa = st.selectbox(
        'Selecione a Etapa do Atendimento',
        ["Selecionar"] + etapa['Etapa'].unique().tolist(),
        index=0
    )

    datas = consulta.datas_atendimentos()
    df_datas = get_datas(datas)
    
    # Converting the 'data' column to datetime format (caso não esteja)
    df_datas['data'] = pd.to_datetime(df_datas['data'], dayfirst=True)

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = df_datas['data'].min()
    maior_data = date.today()

    ##### FILTRO DE INTERVALO DE DATA #####
    intervalo = st.date_input(
        "Selecione um intervalo de datas:",
        value=(menor_data,maior_data)
    )

    # Trata o intervalo de data para busca no banco de dados
    if len(intervalo) == 2:
        data_inicio, data_fim = intervalo
        intervalo_data = f"between '{data_inicio}' and '{data_fim}'"
    else:
        data_inicio = intervalo[0]
        data_fim = datetime.strptime('2030-12-31', "%Y-%m-%d")
        intervalo_data = f"between '{data_inicio}' and '{data_fim}'"

##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1, col_2 = st.columns((1, 8.5))

    with col_1:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2:
        st.title(":blue[Análise dos Clientes]")

##### CORPO DO DASHBOARD #####
with st.container():
    col_1, col_2 = st.columns((1.5, 8.5))
    
    ##### ÁREA DOS CARDS #####
    with col_1:
        st.markdown("### :blue[Clientes Atendidos]")

        ##### CARD TOTAL CLIENTES ÚNICOS #####
        total_clientes_unicos = consulta.total_clientes()
        df_total_clientes_unicos = get_total_clientes_unicos(total_clientes_unicos)
        
        metric_card("Total de Clientes únicos", f"{format(int(df_total_clientes_unicos.shape[0]), ',').replace(',', '.')}")
                                    
        ##### CARD CONTATOS REALIZADOS #####
        qtd_clientes_atendidos = consulta.clientes_atendidos(selectbox_etapa, intervalo_data)
        df_clientes_atendidos = get_qtd_clientes_atendidos(qtd_clientes_atendidos)
        df_clientes_atendidos_card = df_clientes_atendidos.drop_duplicates(subset='CPF')
        
        df_clientes_atendidos_agrupados = df_clientes_atendidos.groupby(['Data', 'Etapa']).size().reset_index(name='Qtd')
        df_clientes_atendidos_agrupados['Data'] = pd.to_datetime(df_clientes_atendidos_agrupados['Data'],format="%d/%m/%Y")

        if df_clientes_atendidos_agrupados.empty:
            datas = pd.date_range(start=data_inicio, end=data_fim)
            etapa = df_clientes_atendidos_agrupados['Etapa'].unique() if not df_clientes_atendidos_agrupados.empty else ['COM_SALDO', 'SEM_SALDO', 'LEAD_NOVO', 'NAO_AUTORIZADO']
            
            combinacoes = list(itertools.product(datas, etapa))

            # Cria o DataFrame com Qtd zerada
            df_clientes_atendidos_agrupados = pd.DataFrame(combinacoes, columns=['Data', 'Etapa'])
            df_clientes_atendidos_agrupados['Qtd'] = 0
            
        metric_card(f'Contatos realizados "{"todos" if selectbox_etapa == "Selecionar" else selectbox_etapa}"', f"{format(int(df_clientes_atendidos.shape[0]), ',').replace(',', '.')}")
        
        ##### CARD % DE ATENDIMENTOS DO TOTAL #####
        valor = f"{(df_clientes_atendidos_card.shape[0] / df_total_clientes_unicos.shape[0] * 100):.2f}".replace('.',',')
        metric_card("% de atendimentos do Total", f"{valor} %")

    ##### ÁREA DA TABELA #####
    with col_2:
        subcol_1, subcol_2 = st.columns((5, 5), gap="medium")


        with subcol_1:
            st.markdown("### :blue[Etapa por Data]")
            chart = (
                alt.Chart(df_clientes_atendidos_agrupados)
                .mark_line(point=True)
                .encode(
                    x=alt.X(
                        'Data:T',
                        title='Data',
                        axis=alt.Axis(format='%d/%m/%Y')
                    ),
                    y=alt.Y(
                        'Qtd:Q',
                        title='Quantidade',
                        scale=alt.Scale(domain=[0, df_clientes_atendidos_agrupados['Qtd'].max() * 1.02])
                    ),
                    color='Etapa:N',
                    tooltip=['Data', 'Etapa', 'Qtd']
                )
                .properties(
                    height=500
                )
            )

            st.altair_chart(chart, use_container_width=True)

        with subcol_2:
            ##### TABELA DE CLIENTES #####
            st.markdown("### :blue[Detalhamento dos Clientes]")
            st.dataframe(df_clientes_atendidos, width='stretch', height=500, hide_index=True)

            ##### BOTÃO EXPORTAR TABELA #####
            csv = df_clientes_atendidos.to_csv(index=False)
            st.download_button(
                label="⬇️ Baixar planilha",
                data=csv,
                file_name="dados.csv",
                mime="text/csv",
            )
