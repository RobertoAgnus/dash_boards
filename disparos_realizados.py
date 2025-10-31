import streamlit as st
import pandas as pd
import itertools
import altair as alt
from datetime import datetime

from querys.querys_sql import QuerysSQL
from querys.connect import Conexao


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Disparos Realizados",
    page_icon="image/logo_agnus.ico",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

##### CRIAR INSTÂNCIA DAS QUERYS #####
consulta_sql = QuerysSQL()

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

@st.cache_resource
def get_connection():
    conectar = Conexao()
    conectar.conectar_postgres()
    conn = conectar.obter_conexao_postgres()
    return conn

@st.cache_data
def get_total_disparos(total):
    conectar = Conexao()
    conectar.conectar_postgres()
    conn = conectar.obter_conexao_postgres()
    df = pd.read_sql_query(total, conn)

    conectar.desconectar_postgres()
    return df

@st.cache_data
def get_status_atendimento(status_atendimento):
    conectar = Conexao()
    conectar.conectar_postgres()
    conn = conectar.obter_conexao_postgres()
    df = pd.read_sql_query(status_atendimento, conn)

    conectar.desconectar_postgres()

    return df

@st.cache_data
def get_qtd_disparos(qtd_disparos):
    conectar = Conexao()
    conectar.conectar_postgres()
    conn = conectar.obter_conexao_postgres()
    df = pd.read_sql_query(qtd_disparos, conn)

    conectar.desconectar_postgres()
    return df

@st.cache_data
def get_disparos(disparos):
    conectar = Conexao()
    conectar.conectar_postgres()
    conn = conectar.obter_conexao_postgres()
    df = pd.read_sql_query(disparos, conn)

    conectar.desconectar_postgres()

    return df

@st.cache_data
def get_datas(datas):
    conectar = Conexao()
    conectar.conectar_postgres()
    conn = conectar.obter_conexao_postgres()
    df = pd.read_sql_query(datas, conn)

    conectar.desconectar_postgres()

    return df

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE STATUS #####
    status_atendimento = consulta_sql.status_disparos()
    status = get_status_atendimento(status_atendimento)
    # status.loc[status['status'] == 'pendente', 'status'] = 'PENDING'

    # Adiciona selectbox status na sidebar:
    selectbox_status = st.selectbox(
        'Selecione o Status do Atendimento',
        ["Selecionar"] + status['status'].unique().tolist(),
        index=0
    )

    datas = consulta_sql.datas_disparos()
    df_datas = get_datas(datas)
    
    # Converting the 'data' column to datetime format (caso não esteja)
    df_datas['data'] = pd.to_datetime(df_datas['data'])

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = df_datas['data'].min()
    maior_data = df_datas['data'].max()

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

    qtd_disparos = consulta_sql.contagem_de_disparos(selectbox_status, intervalo_data)
    total_disparos = get_qtd_disparos(qtd_disparos)

    selectbox_disparos = st.selectbox(
        'Selecione a Quantidade de Disparos',
        ["Selecionar"] + total_disparos['TOTAL'].unique().tolist(),
        index=0
    )

##### GERA O DATAFRAME #####
disparos = consulta_sql.disparos_por_cliente(selectbox_status, intervalo_data, selectbox_disparos)
df_disparados = get_disparos(disparos)

# df_disparados.loc[df_disparados['status'] == 'PENDING', 'status'] = 'Sucesso'
# df_disparados.loc[df_disparados['status'] == 'ERRO', 'status'] = 'Não Recebido'
# df_disparados.loc[df_disparados['status'] == 'CLOSE', 'status'] = 'Vendedor Bloqueado'
# df_disparados.loc[df_disparados['status'] == 'pendente', 'status'] = 'Na fila de disparos'
# df_disparados.loc[df_disparados['status'] == 'DESCONHECIDO', 'status'] = 'Desconhecido'

df_disparados_2 = df_disparados.groupby(['data','telefone','CPF']).size().reset_index(name="Qtd")
df_disparados = pd.merge(df_disparados,df_disparados_2, on=['data','telefone','CPF'], how='left')

if selectbox_disparos != 'Selecionar':
    df_disparados = df_disparados[df_disparados['Qtd'] == selectbox_disparos]

df_disparados = df_disparados[['data','telefone','CPF','status', 'vendedor']].sort_values(['data','CPF'], ascending=[False, True])

df_disparos_agrupados = df_disparados.groupby(['data', 'status']).size().reset_index(name='Qtd')
df_disparos_agrupados['data'] = pd.to_datetime(df_disparos_agrupados['data'],format="%d/%m/%Y")

if df_disparos_agrupados.empty:
    datas = pd.date_range(start=data_inicio, end=data_fim)
    status = df_disparos_agrupados['status'].unique() if not df_disparos_agrupados.empty else ['CLOSE', 'DESCONHECIDO', 'ERRO', 'PENDING', 'pendente']
    
    combinacoes = list(itertools.product(datas, status))

    # Cria o DataFrame com Qtd zerada
    df_disparos_agrupados = pd.DataFrame(combinacoes, columns=['data', 'status'])
    df_disparos_agrupados['Qtd'] = 0

##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1, col_2 = st.columns((1, 8.5))

    with col_1:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2:
        st.title(":blue[Análise dos Disparos]")

with st.container():
    col_1, col_2, col_3, col_4 = st.columns((1.5,1.5,1.5,1.5))
    with col_1:
        total = consulta_sql.total_disparos(intervalo_data)
        df_total_disparos = get_total_disparos(total)

        metric_card("Total de Disparos", f"{format(int(df_total_disparos['total']), ',').replace(',', '.')}")

    with col_2:
        texto = ''
        if selectbox_status == 'Selecionar':
            df_disparos_status = df_disparados
            texto = 'Todos'
        else:    
            df_disparos_status = df_disparados[df_disparados['status'] == selectbox_status]
            texto = selectbox_status

        metric_card(f'Disparos "{texto}"', f"{format(int(df_disparos_status['status'].shape[0]), ',').replace(',', '.')}")

    with col_3:
        valor = f"{(int(df_disparos_status['status'].shape[0]) / int(df_total_disparos['total']) * 100):.2f}".replace('.',',')
        metric_card(f'% do Total', f'{valor} %')

    with col_4:
        st.markdown("### :blue[TESTE 4]")

##### CORPO DO DASHBOARD #####
with st.container():
    col_1, col_2 = st.columns((5, 5), gap="medium")

    with col_1:
        st.markdown("### :blue[Status por Data]")
        chart = (
            alt.Chart(df_disparos_agrupados)
            .mark_line(point=True)
            .encode(
                x=alt.X(
                    'data:T',
                    title='Data',
                    axis=alt.Axis(format='%d/%m/%Y')
                ),
                y=alt.Y(
                    'Qtd:Q',
                    title='Quantidade',
                    scale=alt.Scale(domain=[0, df_disparos_agrupados['Qtd'].max() * 1.02])
                ),
                color='status:N',
                tooltip=['data', 'status', 'Qtd']
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

        ##### BOTÃO EXPORTAR TABELA #####
        csv = df_disparados.to_csv(index=False)
        st.download_button(
            label="⬇️ Baixar planilha",
            data=csv,
            file_name="dados.csv",
            mime="text/csv",
        )
