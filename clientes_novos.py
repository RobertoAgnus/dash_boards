import streamlit as st
import pandas as pd
import altair as alt

# from querys.querys_sql import QuerysSQL
# from querys.connect import Conexao

## REMOVER QUANDO FOR PARA PRODUÇÃO ##
from querys.querys_csv import QuerysCSV
import duckdb as dk


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Clientes Novos",
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
consulta = QuerysCSV()


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
            <h3 style="color: white; font-size: 40px">{value}</h3>
        </div>
        """,
        unsafe_allow_html=True
    )

@st.cache_data
def get_qtd_clientes_novos(qtd_clientes_novos):
    # df = pd.read_sql(qtd_clientes_novos, conn)
    df = dk.query(qtd_clientes_novos).to_df()

    return df

@st.cache_data
def get_qtd_clientes(qtd_clientes):
    # df  = pd.read_sql(qtd_clientes, conn)
    df  = dk.query(qtd_clientes).to_df()

    return df

@st.cache_data
def get_clientes_novos(clientes_novos):
    df = dk.query(clientes_novos).to_df()

    return df

@st.cache_data
def get_clientes_recorrentes(clientes_recorrentes):
    df = dk.query(clientes_recorrentes).to_df()

    return df


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
        st.markdown("#### :blue[Quantidade de Clientes]")
        
        ##### CARD CLIENTES NOVOS #####
        qtd_clientes_novos = consulta.qtd_clientes_novos()
        cd_clientes_novos = get_qtd_clientes_novos(qtd_clientes_novos)
        
        metric_card("Clientes Novos", f"{format(int(cd_clientes_novos.shape[0]), ",").replace(",", ".")}")

        ##### CARD TOTAL DE CLIENTES #####
        qtd_clientes = consulta.qtd_clientes()
        cd_clientes  = get_qtd_clientes(qtd_clientes)

        metric_card("Total de Clientes", f"{format(int(cd_clientes.shape[0]), ',').replace(',', '.')}")

        ##### CARD % CLIENTES NOVOS DO TOTAL #####
        valor = f"{(cd_clientes_novos.shape[0] / cd_clientes.shape[0] * 100):.2f}".replace('.',',')
        metric_card("% de Clientes Novos do Total", f"{valor} %")

    ##### ÁREA DA TABELA #####
    with col_2:
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

            tb_clientes_recorrentes = tb_clientes_recorrentes[['Inclusão','Inclusão Corban','CPF','Nome','Telefone']]

            tb_clientes_recorrentes['Inclusão'] = pd.to_datetime(tb_clientes_recorrentes['Inclusão'], errors='coerce').dt.strftime("%d/%m/%Y")

            tb_clientes_recorrentes['Inclusão Corban'] = pd.to_datetime(tb_clientes_recorrentes['Inclusão Corban'], errors='coerce').dt.strftime("%d/%m/%Y")

            st.session_state.df = tb_clientes_recorrentes
            
        else:
            st.markdown("#### :blue[Detalhamento de todos os Clientes]")
            
            clientes_novos = consulta.clientes_novos(selectbox_tipo_cliente)
            tb_clientes_todos = get_clientes_novos(clientes_novos)

            tb_clientes_todos = tb_clientes_todos[['Inclusão','Inclusão Corban','CPF','Nome','Telefone']]

            tb_clientes_todos['Inclusão'] = pd.to_datetime(tb_clientes_todos['Inclusão'], errors='coerce').dt.strftime("%d/%m/%Y")

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

##### FECHAR A CONEXÃO #####
# conectar.desconectar()
