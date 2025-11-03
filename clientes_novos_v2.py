import streamlit as st
import pandas as pd
import altair as alt

from querys.querys_sql import QuerysSQL
from querys.connect import Conexao


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Clientes Novos",
    page_icon="image/logo_agnus.ico",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")


##### CRIAR INSTÂNCIA ÚNICA #####
consulta = QuerysSQL()

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE CONTRATO #####
    selectbox_tipo_cliente = st.selectbox(
        'Selecione o tipo de Cliente',
        ["Todos", 'Sem Contrato', 'Com Contrato'],
        index=0
    )

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
def get_qtd_clientes_novos(qtd_clientes_novos):
    conectar = Conexao()
    conectar.conectar_mysql()
    conn = conectar.obter_conexao_mysql()
    df = pd.read_sql(qtd_clientes_novos, conn)
    
    conectar.desconectar_mysql()

    return df

@st.cache_data
def get_qtd_clientes(qtd_clientes):
    conectar = Conexao()
    conectar.conectar_mysql()
    conn = conectar.obter_conexao_mysql()
    df  = pd.read_sql(qtd_clientes, conn)
    
    conectar.desconectar_mysql()

    return df

@st.cache_data
def get_clientes_novos(clientes_novos):
    conectar = Conexao()
    conectar.conectar_mysql()
    conn = conectar.obter_conexao_mysql()
    df = pd.read_sql(clientes_novos, conn)

    conectar.desconectar_mysql()

    return df

@st.cache_data
def get_clientes_recorrentes(clientes_recorrentes):
    conectar = Conexao()
    conectar.conectar_mysql()
    conn = conectar.obter_conexao_mysql()
    df = pd.read_sql(clientes_recorrentes, conn)

    conectar.desconectar_mysql()

    return df

##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1, col_2 = st.columns((2, 8))

    with col_1:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2:
        st.title(":blue[Análise dos Clientes]")

##### CORPO DO DASHBOARD #####
with st.container():
    col_1a, col_1b, col_1c = st.columns((3.3, 3.3, 3.3))
    
    ##### ÁREA DOS CARDS #####
    with col_1a:
        
        ##### CARD TOTAL DE CLIENTES #####
        qtd_clientes = consulta.qtd_clientes()
        cd_clientes  = get_qtd_clientes(qtd_clientes)

        metric_card("Total de Clientes", f"{format(int(cd_clientes['total']), ',').replace(',', '.')}")

    with col_1b:
        ##### CARD CLIENTES NOVOS #####
        qtd_clientes_novos = consulta.qtd_clientes_novos()
        cd_clientes_novos = get_qtd_clientes_novos(qtd_clientes_novos)
        
        metric_card("Clientes Novos", f"{format(int(cd_clientes_novos['total']), ',').replace(',', '.')}")

    with col_1c:
        ##### CARD % CLIENTES NOVOS DO TOTAL #####
        valor = f"{(int(cd_clientes_novos['total']) / int(cd_clientes['total']) * 100):.2f}".replace('.',',')
        metric_card("% de Clientes Novos do Total", f"{valor} %")

##### ÁREA DA TABELA #####
with st.container():
    ##### TABELA DE CLIENTES #####
    if selectbox_tipo_cliente == "Sem Contrato":
        st.markdown("#### :blue[Detalhamento dos Clientes Novos]")
        
        clientes_novos = consulta.clientes_novos(selectbox_tipo_cliente)
        tb_clientes_novos = get_clientes_novos(clientes_novos)

        tb_clientes_novos = tb_clientes_novos[['CPF','Nome','Telefone']]

        tb_clientes_novos = tb_clientes_novos.drop_duplicates(subset='CPF')
        
        st.session_state.df = tb_clientes_novos
        
    elif selectbox_tipo_cliente == "Com Contrato":
        st.markdown("#### :blue[Detalhamento dos Clientes Recorrentes]")
        
        clientes_recorrentes = consulta.clientes_novos(selectbox_tipo_cliente)
        tb_clientes_recorrentes = get_clientes_recorrentes(clientes_recorrentes)

        tb_clientes_recorrentes = tb_clientes_recorrentes[['Inclusão CRM','Inclusão Corban','CPF','Nome','Telefone']]

        tb_clientes_recorrentes['Inclusão CRM'] = pd.to_datetime(tb_clientes_recorrentes['Inclusão CRM'], errors='coerce').dt.strftime("%d/%m/%Y")

        tb_clientes_recorrentes['Inclusão Corban'] = pd.to_datetime(tb_clientes_recorrentes['Inclusão Corban'], errors='coerce').dt.strftime("%d/%m/%Y")

        st.session_state.df = tb_clientes_recorrentes
        
    else:
        st.markdown("#### :blue[Detalhamento de todos os Clientes]")
        
        clientes_novos = consulta.clientes_novos(selectbox_tipo_cliente)
        tb_clientes_todos = get_clientes_novos(clientes_novos)

        tb_clientes_todos = tb_clientes_todos[['Inclusão CRM','Inclusão Corban','CPF','Nome','Telefone']]

        tb_clientes_todos['Inclusão CRM'] = pd.to_datetime(tb_clientes_todos['Inclusão CRM'], errors='coerce').dt.strftime("%d/%m/%Y")

        tb_clientes_todos['Inclusão Corban'] = pd.to_datetime(tb_clientes_todos['Inclusão Corban'], errors='coerce').dt.strftime("%d/%m/%Y")

        st.session_state.df = tb_clientes_todos
        
    st.dataframe(st.session_state.df, height=500, hide_index=True)

    ##### BOTÃO EXPORTAR TABELA #####
    csv = st.session_state.df.to_csv(index=False)
    st.download_button(
        label="⬇️ Baixar planilha",
        data=csv,
        file_name="dados.csv",
        mime="text/csv",
    )
