import streamlit as st
import pandas as pd
import altair as alt
import duckdb as dk

from querys.querys_sql import QuerysSQL
from querys.connect import Conexao

##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Base FGTS -> CLT",
    page_icon="image/logo_agnus.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)
alt.themes.enable("dark")

dk.execute("PRAGMA memory_limit='8GB';")

##### INSTÂNCIAS ÚNICAS #####
consulta_sql = QuerysSQL()

##### FUNÇÕES AUXILIARES #####
def tratar_numero(num):
    """Normaliza números de telefone."""
    if pd.isna(num):
        return None
    num = str(num)
    if len(num) > 11 and num.startswith("55"):
        num = num[2:]
    if len(num) < 11:
        num = num[:2] + "9" + num[2:]
    return num

def metric_card(label, value):
    st.markdown(
        f"""
        <div style="background-color:#262730;border-radius:10px;text-align:center;margin-bottom:15px;padding:10px;">
            <p style="color:white;font-weight:bold;">{label}</p>
            <h3 style="color:white;font-size: calc(1rem + 1vw);">{value}</h3>
        </div>
        """, unsafe_allow_html=True
    )

##### CACHE DE CONSULTAS #####
@st.cache_data(show_spinner=False)
def carregar_dados():
    """Executa todas as consultas necessárias apenas uma vez."""
    conectar = Conexao()
    
    conectar.conectar_mysql()
    conn_mysql = conectar.obter_conexao_mysql()

    conectar.conectar_postgres()
    conn_postgres = conectar.obter_conexao_postgres()
    
    try:
        conn_mysql.ping(reconnect=True)  # reabre se estiver desconectada
        
        telefones_corban = consulta_sql.obtem_telefones()
    
        df_telefones_corban = pd.read_sql_query(telefones_corban, conn_postgres)
        df_telefones_corban["telefoneAPICorban"] = df_telefones_corban["telefoneAPICorban"].map(tratar_numero)

        # Dicionário de condições SQL
        consultas = {
            "Contratados": "numContrato IS NOT NULL",
            "+3 Meses": "dataInclusao < (NOW() - INTERVAL 3 MONTH)",
            "Sem Contrato": "numContrato IS NULL",
            "Sem CPF": None
        }

        dados = {}
        for nome, cond in consultas.items():
            if nome == "Sem CPF":
                query = consulta_sql.clientes_sem_cpf()
            else:
                query = consulta_sql.consulta_base_fgts(cond)
            df = pd.read_sql(query, conn_mysql)

            # Tratamento de telefones em lote (mais rápido que apply em Python puro)
            for col in ["telefone", "telefoneLeads"]:
                if col in df.columns:
                    df[col] = df[col].map(tratar_numero)

            # Formatação numérica (vetorizada)
            for col in ["valorFinanciado", "valorLiberado"]:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

            # Join com base de telefones
            if nome != "Sem CPF":
                df = df.merge(df_telefones_corban, on="CPF", how="left")

            # Armazena resultado
            dados[nome] = df

        return dados
        
    except Exception as e:
        st.error(f"Erro de conexão: {e}")

    conectar.desconectar_mysql()
    conectar.desconectar_postgres()


##### INTERFACE LATERAL #####
with st.sidebar:
    if "filtro_perfil" not in st.session_state:
        st.session_state.filtro_perfil = "Contratados"

    st.title("Filtros")
    selectbox_perfil = st.selectbox(
        "Selecione o Perfil do Cliente",
        ["Contratados", "+3 Meses", "Sem Contrato", "Sem CPF"],
        key="filtro_perfil"
    )

    # Botão de limpeza
    if st.button("🧹 Limpar filtros"):
        st.session_state.clear()
        st.rerun()

##### CARREGAR OS DADOS (1x) #####
dados = carregar_dados()

##### DADOS SELECIONADOS #####
df_tabela = dados[selectbox_perfil]
texto_tabela = selectbox_perfil

##### CONTADORES #####
contagens = {
                nome: (
                        len(df.drop_duplicates(subset="CPF")) 
                        if nome != "Sem CPF" 
                        else len(df.drop_duplicates(subset="telefone"))
                    ) 
                    for nome, df in dados.items()
            }

##### CABEÇALHO #####
col_1, col_2 = st.columns((2, 8.5))
with col_1:
    st.image("image/logo_agnus.jpg", width=200)
with col_2:
    st.title(":blue[Análise dos Clientes]")

##### CARDS E TABELA #####
with st.container():
    col_1, col_2, col_3, col_4 = st.columns((2.5, 2.5, 2.5, 2.5))
    colunas = [col_1, col_2, col_3, col_4]
    count = 0
    for k, v in contagens.items():
        with colunas[count]:
            metric_card(f"Clientes {k}", f"{v:,}".replace(",", "."))
        count += 1

with st.container():
    st.markdown(f"### :blue[Detalhamento dos Clientes {texto_tabela}]")

    if selectbox_perfil != 'Sem CPF':
        df_tabela['dataInclusao'] = pd.to_datetime(df_tabela['dataInclusao'])
        df_tabela['dataInclusao'] = df_tabela['dataInclusao'].dt.strftime('%d/%m/%Y')
        
        df_tabela['dataPagamento'] = pd.to_datetime(df_tabela['dataPagamento'])
        df_tabela['dataPagamento'] = df_tabela['dataPagamento'].dt.strftime('%d/%m/%Y')
    
    st.dataframe(df_tabela, use_container_width=True, height=500, hide_index=True)

    csv = df_tabela.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Baixar planilha",
        data=csv,
        file_name=f"clientes_{texto_tabela}.csv",
        mime="text/csv"
    )
