import streamlit as st
import pandas as pd
import altair as alt
from datetime import date, datetime

# from querys.querys_sql import QuerysSQL
# from querys.connect import Conexao

## REMOVER QUANDO FOR PARA PRODUÇÃO ##
from querys.querys_csv import QuerysSQLcsv
import duckdb as dk


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Disparos Realizados",
    page_icon="🏂",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

##### CONEXÃO COM O BANCO DE DADOS #####
# # Criar uma instância da classe Conexao
# conectar = Conexao()
# conectar.conectar()

# # Conectando ao banco de dados MySQL
# conn = conectar.obter_conexao()


##### CRIAR INSTÂNCIA DO BANCO #####
consulta = QuerysSQLcsv()

@st.cache_data
def get_status_atendimento():
    status_atendimento = consulta.status_atendimentos()
    df = dk.query(status_atendimento).to_df()

    return df

@st.cache_data
def get_qtd_disparos(selectbox_status, intervalo_data):
    qtd_disparos = consulta.contagem_de_disparos(selectbox_status, intervalo_data)
    # df = pd.read_sql(qtd_disparos, conn)
    df = dk.query(qtd_disparos).to_df()

    return df

@st.cache_data
def get_disparos(selectbox_status, intervalo_data, selectbox_disparos):
    disparos = consulta.disparos_por_cliente(selectbox_status, intervalo_data, selectbox_disparos)
    df = dk.query(disparos).to_df()

    return df

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE STATUS #####
    status = get_status_atendimento()
    status = status[status['Status'].isin(['LEAD_NOVO', 'SEM_SALDO', 'NAO_AUTORIZADO', 'COM_SALDO'])]


    # Adiciona selectbox status na sidebar:
    selectbox_status = st.selectbox(
        'Selecione o Status do Atendimento',
        ["Selecionar"] + status['Status'].unique().tolist(),
        index=0
    )

    ##### FILTRO DE INTERVALO DE DATA #####
    intervalo = st.date_input(
        "Selecione um intervalo de datas:",
        value=(date(2020, 1, 1),date(2030, 12, 31))
    )

    # Trata o intervalo de data para busca no banco de dados
    if len(intervalo) == 2:
        data_inicio, data_fim = intervalo
        intervalo_data = f"between '{data_inicio}' and '{data_fim}'"
    else:
        data_inicio = intervalo[0]
        data_fim = datetime.strptime('2030-12-31', "%Y-%m-%d")
        intervalo_data = f"between '{data_inicio}' and '{data_fim}'"

    total_disparos = get_qtd_disparos(selectbox_status, intervalo_data)

    selectbox_disparos = st.selectbox(
        'Selecione a Quantidade de Disparos',
        ["Selecionar"] + total_disparos['TOTAL'].unique().tolist(),
        index=0
    )

##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1, col_2 = st.columns((1, 8.5))

    with col_1:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2:
        st.title(":blue[Análise dos clientes atendidos no sistema]")

##### CORPO DO DASHBOARD #####
with st.container():
    col_1, col_2 = st.columns((5, 5), gap="medium")

    ##### GERA O DATAFRAME #####
    df_disparados = get_disparos(selectbox_status, intervalo_data, selectbox_disparos)

    df_disparados = df_disparados[df_disparados['Status'].isin(['LEAD_NOVO', 'SEM_SALDO', 'NAO_AUTORIZADO', 'COM_SALDO'])]

    df_disparados_2 = df_disparados.groupby(['Data','Telefone']).size().reset_index(name="Qtd")
    df_disparados = pd.merge(df_disparados,df_disparados_2, on=['Data','Telefone'], how='left')

    if selectbox_disparos != 'Selecionar':
        df_disparados = df_disparados[df_disparados['Qtd'] == selectbox_disparos]

    df_disparados = df_disparados[['Data','Telefone','CPF','Nome','Status']]

    df_disparos_agrupados = df_disparados.groupby(['Data', 'Status']).size().reset_index(name='Qtd')
    df_disparos_agrupados['Data'] = pd.to_datetime(df_disparos_agrupados['Data'],format="%d/%m/%Y") #.dt.date
    

    df_pivot = df_disparos_agrupados.pivot_table(
        index='Data',
        columns='Status',
        values='Qtd',
        aggfunc='sum'
    ).fillna(0)

    with col_1:
        st.markdown("### :blue[Status por Data]")
        chart = (
            alt.Chart(df_disparos_agrupados)
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
                    scale=alt.Scale(domain=[0, df_disparos_agrupados['Qtd'].max() * 1.02])
                ),
                color='Status:N',
                tooltip=['Data', 'Status', 'Qtd']
            )
            .properties(
                height=500
            )
        )

        st.altair_chart(chart, use_container_width=True)

    with col_2:
        ##### TABELA DE CLIENTES #####
        st.markdown("### :blue[Disparos por Clientes]")
        st.dataframe(df_disparados, height=500, hide_index=True)
        # df_disparados.columns

        ##### BOTÃO EXPORTAR TABELA #####
        csv = df_disparados.to_csv(index=False)
        st.download_button(
            label="⬇️ Baixar planilha",
            data=csv,
            file_name="dados.csv",
            mime="text/csv",
        )

    # st.write(df_disparados['Status'].unique())
##### FECHAR A CONEXÃO #####
# conectar.desconectar()