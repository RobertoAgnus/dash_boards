from datetime import datetime
import streamlit as st
import pandas as pd
import altair as alt

from querys.querys_sql import QuerysSQL
from querys.connect import Conexao

# from querys.querys_csv import QuerysCSV
# import duckdb as dk

##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Comissões Corban",
    page_icon="image/logo_agnus.ico",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

# dk.execute("PRAGMA memory_limit='8GB';")

##### CONEXÃO COM O BANCO DE DADOS #####
# Criar uma instância da classe Conexao
# conectar = Conexao()
# conectar.conectar_postgres()

# Conectando ao banco de dados PostgreSQL
# conn = conectar.obter_conexao_postgres()

##### CRIAR INSTÂNCIA DO BANCO #####
consulta_sql = QuerysSQL()
# consulta_csv = QuerysCSV()

@st.cache_data
def get_data_proposta(data_proposta):
    conectar = Conexao()
    conectar.conectar_postgres()
    conn = conectar.obter_conexao_postgres()
    df = pd.read_sql_query(data_proposta, conn)
    
    conectar.desconectar_postgres()

    return df

@st.cache_data
def get_origem_proposta(origem_proposta):
    conectar = Conexao()
    conectar.conectar_postgres()
    conn = conectar.obter_conexao_postgres()
    df = pd.read_sql_query(origem_proposta, conn)
    
    conectar.desconectar_postgres()

    return df

@st.cache_data
def get_corban(corban):
    conectar = Conexao()
    conectar.conectar_postgres()
    conn = conectar.obter_conexao_postgres()
    df = pd.read_sql_query(corban, conn)
    
    conectar.desconectar_postgres()

    return df

##### INTERVALO DE DATA DO ARQUIVO #####
data_proposta = consulta_sql.data_proposta_corban()
data = get_data_proposta(data_proposta)

# Converting the 'data' column to datetime format (caso não esteja)
data['data'] = pd.to_datetime(data['data'])

# Obtendo a menor e a maior data da coluna 'data'
menor_data = data['data'].min()
maior_data = data['data'].max()

##### ORIGEM DAS PROPOSTAS CONTRATADAS #####
origem_proposta = consulta_sql.origem_proposta_corban()
origem = get_origem_proposta(origem_proposta)

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE ORIGEM #####
    selectbox_origem = st.selectbox(
        'Selecione a origem',
        ['Selecionar'] + origem['origem'].unique().tolist(),
        index=0
    )
    
    ##### FILTRO DE DATA #####
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
    
##### OBTEM TABELA DE COMISSOES #####
corban = consulta_sql.tabela_corban(selectbox_origem, intervalo_data)
df_comissao = get_corban(corban)

df_comissao['Data da Comissão'] = pd.to_datetime(df_comissao['Data da Comissão']).dt.strftime('%d/%m/%Y')

df_comissao['Valor'] = df_comissao['Valor'].astype(float)

df_comissao_agrupado = df_comissao.groupby(['Data da Comissão', 'Origem', 'Status'], as_index=False).agg({'Valor': 'sum'})

total = pd.DataFrame({"Categoria": ["Total"], "Valor": [df_comissao_agrupado["Valor"].sum()]})

total_metric = f'{df_comissao_agrupado['Valor'].sum():.2f}'

df_comissao_pago = df_comissao_agrupado[df_comissao_agrupado['Status'] == 'Pago']
total_pago = f'{df_comissao_pago['Valor'].sum():.2f}'

df_comissao_aguardando = df_comissao_agrupado[(df_comissao_agrupado['Status'] == 'Aguardando Pagamento') | (df_comissao_agrupado['Status'] == '')]
total_aguardando = f'{df_comissao_aguardando['Valor'].sum():.2f}'

total_qtd = df_comissao_agrupado['Status'].count()

df_comissao_cancelado = df_comissao_agrupado[df_comissao_agrupado['Status'] == 'Cancelado']
total_cancelado = df_comissao_cancelado['Status'].count()

total_qtd_conversao = f'{((total_qtd - total_cancelado) / total_qtd) * 100:.2f}'

##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1, col_2 = st.columns((1, 8.5))

    with col_1:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2:
        st.title(":blue[Análise das Comissões]")

##### CORPO DO DASHBOARD #####
with st.container():
    col_1, col_2 = st.columns((2, 8.5), gap="medium")   #, col_3 = st.columns((2, 2, 8.5), gap="medium")

    ##### ÁREA DOS CARDS #####
    with col_1:
        st.markdown("### :blue[Valor Comissões]")
        valor_total = f"R$ {format(float(total_metric), ',.2f').replace('.','|').replace(',','.').replace('|',',')}"
        metric_card("Valor Total", f"{valor_total}")

        valor_pago = f"R$ {format(float(total_pago), ',.2f').replace('.','|').replace(',','.').replace('|',',')}"
        metric_card("Valor Pago", f"{valor_pago}")

        valor_aguardando = f"R$ {format(float(total_aguardando), ',.2f').replace('.','|').replace(',','.').replace('|',',')}"
        metric_card("Valor Aguardando", f"{valor_aguardando}")

        # qtd_cancelado = f"{int(total_qtd_conversao)}"
        metric_card("Conversão", f"{format(float(total_qtd_conversao), ',.2f').replace('.',',')} %")

    # with col_2:
    #     st.markdown("### :blue[Valor Comissões]")
    #     valor_total = f"R$ {format(float(total_metric), ',.2f').replace('.','|').replace(',','.').replace('|',',')}"
    #     metric_card("Valor Total", f"{valor_total}")

    #     valor_pago = f"R$ {format(float(total_pago), ',.2f').replace('.','|').replace(',','.').replace('|',',')}"
    #     metric_card("Valor Pago", f"{valor_pago}")

    #     valor_aguardando = f"R$ {format(float(total_aguardando), ',.2f').replace('.','|').replace(',','.').replace('|',',')}"
    #     metric_card("Valor Aguardando", f"{valor_aguardando}")

    #     # qtd_cancelado = f"{int(total_qtd_conversao)}"
    #     metric_card("Conversão", f"{format(float(total_qtd_conversao), ',.2f').replace('.',',')} %")
        
##### ÁREA DA TABELA #####
    with col_2:
        ##### TABELA DE CLIENTES #####
        st.markdown("### :blue[Detalhamento das Comissões]")
        st.dataframe(df_comissao_agrupado, height=500, hide_index=True)

        ##### BOTÃO EXPORTAR TABELA #####
        csv = df_comissao_agrupado.to_csv(index=False)
        st.download_button(
            label="⬇️ Baixar planilha",
            data=csv,
            file_name="dados.csv",
            mime="text/csv",
        )
