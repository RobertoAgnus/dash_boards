import streamlit as st
import altair as alt
from datetime import date, datetime

# from querys.querys_sql import QuerysSQL
# from querys.connect import Conexao

## REMOVER QUANDO FOR PARA PRODUÇÃO ##
from querys.querys_csv import QuerysSQLcsv
import duckdb as dk


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Clientes Atendidos",
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
def get_total_clientes_unicos():
    total_clientes_unicos = consulta.total_clientes()
    # df = pd.read_sql(total_clientes, conn)
    df = dk.query(total_clientes_unicos).to_df()

    return df

@st.cache_data
def get_qtd_clientes_atendidos(selectbox_status, intervalo_data):
    qtd_clientes_atendidos = consulta.clientes_atendidos(selectbox_status, intervalo_data)
    # df = pd.read_sql(qtd_clientes_atendidos, conn)
    df = dk.query(qtd_clientes_atendidos).to_df()

    return df

@st.cache_data
def get_status_atendimento():
    status_atendimento = consulta.status_atendimentos()
    df = dk.query(status_atendimento).to_df()

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
        st.markdown("### :blue[Clientes Atendidos]")

        ##### CARD TOTAL CLIENTES ÚNICOS #####
        df_total_clientes_unicos = get_total_clientes_unicos()
        
        metric_card("Total de Clientes únicos", f"{format(int(df_total_clientes_unicos.shape[0]), ',').replace(',', '.')}")
                                    
        ##### CARD CONTATOS REALIZADOS #####
        df_clientes_atendidos = get_qtd_clientes_atendidos(selectbox_status, intervalo_data)
        df_clientes_atendidos_card = df_clientes_atendidos.drop_duplicates(subset='CPF')
        
        metric_card(f'Contatos realizados "{"todos" if selectbox_status == "Selecionar" else selectbox_status}"', f"{format(int(df_clientes_atendidos.shape[0]), ',').replace(',', '.')}")
        
        ##### CARD % DE ATENDIMENTOS DO TOTAL #####
        valor = f"{(df_clientes_atendidos_card.shape[0] / df_total_clientes_unicos.shape[0] * 100):.2f}".replace('.',',')
        metric_card("% de atendimentos do Total", f"{valor} %")

    ##### ÁREA DA TABELA #####
    with col_2:
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

##### FECHAR A CONEXÃO #####
# conectar.desconectar()
        