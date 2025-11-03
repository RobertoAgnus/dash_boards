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

##### CACHE DE CONSULTAS #####
@st.cache_data(show_spinner=False)
def carregar_dados():
    conectar = Conexao()
    conectar.conectar_mysql()
    conn = conectar.obter_conexao_mysql()

    qtd_clientes_total = consulta.qtd_clientes_total()
    df_clientes_total  = pd.read_sql(qtd_clientes_total, conn)

    clientes_novos = consulta.clientes_novos()
    df_clientes_novos = pd.read_sql(clientes_novos, conn)

    conectar.desconectar_mysql()

    return {"qtd_total": df_clientes_total, "clientes_novos": df_clientes_novos}

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE CONTRATO #####
    selectbox_tipo_cliente = st.selectbox(
        'Selecione o tipo de Cliente',
        ["Todos", 'Sem Contrato', 'Com Contrato'],
        index=0
    )

dados = carregar_dados()

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
        df_clientes = dados['qtd_total']

        metric_card("Total de Clientes", f"{format(int(df_clientes['total']), ',').replace(',', '.')}")

    with col_1b:
        ##### CARD CLIENTES NOVOS #####
        df_qtd_clientes_novos = dados['clientes_novos'].drop_duplicates(subset="CPF")

        df_qtd_clientes_novos = df_qtd_clientes_novos[(df_qtd_clientes_novos["Inclusão CRM"].isnull()) & (df_qtd_clientes_novos["Inclusão Corban"].isnull())]
        
        metric_card("Clientes Novos", f"{format(int(df_qtd_clientes_novos.shape[0]), ',').replace(',', '.')}")

    with col_1c:
        ##### CARD % CLIENTES NOVOS DO TOTAL #####
        valor = f"{(int(df_qtd_clientes_novos.shape[0]) / int(df_clientes['total']) * 100):.2f}".replace('.',',')
        metric_card("% de Clientes Novos do Total", f"{valor} %")

##### ÁREA DA TABELA #####
with st.container():
    
    ##### TABELA DE CLIENTES #####
    df_clientes_novos = dados['clientes_novos']

    if selectbox_tipo_cliente == "Sem Contrato":
        st.markdown("#### :blue[Detalhamento dos Clientes Novos]")
        

        tb_clientes_novos = df_clientes_novos.drop_duplicates(subset='CPF')

        tb_clientes_novos = tb_clientes_novos[(tb_clientes_novos["Inclusão CRM"].isnull()) & (tb_clientes_novos["Inclusão Corban"].isnull())]
        
        tb_clientes_novos = tb_clientes_novos[['CPF','Nome','Telefone']]
        
        st.session_state.df = tb_clientes_novos
        
    elif selectbox_tipo_cliente == "Com Contrato":
        st.markdown("#### :blue[Detalhamento dos Clientes Recorrentes]")
        
        tb_clientes_recorrentes = df_clientes_novos[['Inclusão CRM','Inclusão Corban','CPF','Nome','Telefone']]

        tb_clientes_recorrentes['Inclusão CRM'] = pd.to_datetime(tb_clientes_recorrentes['Inclusão CRM'], errors='coerce').dt.strftime("%d/%m/%Y")

        tb_clientes_recorrentes['Inclusão Corban'] = pd.to_datetime(tb_clientes_recorrentes['Inclusão Corban'], errors='coerce').dt.strftime("%d/%m/%Y")

        tb_clientes_recorrentes = tb_clientes_recorrentes[(~tb_clientes_recorrentes["Inclusão CRM"].isnull()) | (~tb_clientes_recorrentes["Inclusão Corban"].isnull())]

        st.session_state.df = tb_clientes_recorrentes
        
    else:
        st.markdown("#### :blue[Detalhamento de todos os Clientes]")
        
        tb_clientes_todos = df_clientes_novos[['Inclusão CRM','Inclusão Corban','CPF','Nome','Telefone']]

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
