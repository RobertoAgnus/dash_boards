import streamlit as st
import pandas as pd
import altair as alt

# from querys.querys_sql import QuerysSQL
# from querys.connect import Conexao

## REMOVER QUANDO FOR PARA PRODUÇÃO ##
from querys.querys_csv import QuerysSQL
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
            <h3 style="color: white; font-size: 40px">{value}</h3>
        </div>
        """,
        unsafe_allow_html=True
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
    col_1, col_2 = st.columns((1.5, 8.5))
    
    ##### ÁREA DOS CARDS #####
    with col_1:
        st.markdown("#### :blue[Quantidade de Clientes]")
        
        ##### CARD CLIENTES NOVOS #####
        qtd_clientes_novos = consulta.qtd_clientes_novos()
        # cd_clientes_novos = pd.read_sql(qtd_clientes_novos, conn)
        cd_clientes_novos = dk.query(qtd_clientes_novos).to_df()
        
        metric_card("Clientes Novos", f"{format(int(cd_clientes_novos.shape[0]), ",").replace(",", ".")}")

        ##### CARD TOTAL DE CLIENTES #####
        qtd_clientes = consulta.qtd_clientes()
        # cd_clientes  = pd.read_sql(qtd_clientes, conn)
        cd_clientes  = dk.query(qtd_clientes).to_df()

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
            tb_clientes_novos = dk.query(clientes_novos).to_df()

            tb_clientes_novos = tb_clientes_novos[['CPF','Nome','Telefone']]

            tb_clientes_novos = tb_clientes_novos.drop_duplicates(subset='CPF')
            
            st.session_state.df = tb_clientes_novos
            
        elif selectbox_tipo_cliente == "Com Contrato":
            st.markdown("#### :blue[Detalhamento dos Clientes Recorrentes]")
            clientes_recorrentes = consulta.clientes_novos(selectbox_tipo_cliente)
            tb_clientes_recorrentes = dk.query(clientes_recorrentes).to_df()

            tb_clientes_recorrentes = tb_clientes_recorrentes[['Inclusão','Inclusão Corban','CPF','Nome','Telefone']]

            tb_clientes_recorrentes['Inclusão'] = pd.to_datetime(tb_clientes_recorrentes['Inclusão'], errors='coerce').dt.strftime("%d/%m/%Y")

            tb_clientes_recorrentes['Inclusão Corban'] = pd.to_datetime(tb_clientes_recorrentes['Inclusão Corban'], errors='coerce').dt.strftime("%d/%m/%Y")

            st.session_state.df = tb_clientes_recorrentes
            
        else:
            st.markdown("#### :blue[Detalhamento de todos os Clientes]")
            clientes_todos = consulta.clientes_novos(selectbox_tipo_cliente)
            tb_clientes_todos = dk.query(clientes_todos).to_df()

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
