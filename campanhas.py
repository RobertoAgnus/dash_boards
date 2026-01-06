import streamlit as st
import altair as alt
import pandas as pd
import numpy as np
import unicodedata
from datetime import date
from io import BytesIO
import io
import zipfile

from querys.connect import Conexao
from querys.querys_sql import QuerysSQL
from regras.formatadores import formatar_cpf, formatar_telefone


if not st.session_state.get("authenticated", False):
    st.stop()

if "authenticated" not in st.session_state:
    st.error("Acesso negado. Faça login.")
    st.stop()


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Campanhas Publicitárias",
    page_icon="image/logo_agnus.ico",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

##### FUNÇÃO PARA REMOVER EMOJIS #####
def remover_emojis(texto: str) -> str:
    if not isinstance(texto, str):
        return texto
    return "".join(
        c for c in texto
        if not unicodedata.category(c).startswith("So")
    )

##### FUNÇÃO PARA FORMATAR FLOAT #####
def formata_float(valor):
    return f"{valor:,.2f}".replace('.','|').replace(',','.').replace('|',',').replace('nan', '0,00')

##### FUNÇÃO PARA MAPEAR MENSAGENS #####
def mapeia_mensagens(mensagem):
    if '[' in mensagem:
        return mensagem.split(']')[0] + ']'
    elif '(s' in mensagem:
        return mensagem.split(')')[0] + ')'
    elif ('Falar com atendente' in mensagem) or ('Falar com suporte' in mensagem) or ('Ver atualização' in mensagem) or ('Receber proposta' in mensagem):
        return 'Disparos'
    elif ('Olá! Gostaria de fazer' in mensagem) or ('Olá, quero antecipar' in mensagem):
        return 'Site'
    else:
        return 'Orgânico'
    
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

##### FUNÇÃO PARA OBTER AS DATAS CONSULTA #####
def get_datas_liberacao(df):
    # Remove linhas com Data Consulta vazia
    df = df.dropna(subset=['Data da Liberação'])

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = df['Data da Liberação'].min()
    maior_data = date.today()
    
    return menor_data, maior_data

##### CARREGAR OS DADOS (1x) #####
conectar = Conexao()

conectar.conectar_postgres_aws()

conn_postgres_aws = conectar.obter_conexao_postgres_aws()

consulta = QuerysSQL()

crm = consulta.get_campanhas()

df_crm     = pd.read_sql_query(crm, conn_postgres_aws)

df_crm['valorTotalComissao'] = np.where(df_crm['dataPagamento'].isna(), 0, df_crm['valorTotalComissao'])

df_crm = formatar_cpf(df_crm, 'cpf')
df_crm = formatar_telefone(df_crm, 'numero')

df_crm = df_crm.rename(columns={
    'numero':'Telefone',
    'cpf':'CPF',
    'nome':'Nome',
    'createdAt':'Data da Mensagem',
    'mensagemInicial':'Mensagem Inicial',
    'nome_banco':'Banco',
    'dataPagamento':'Data da Liberação',
    'valorBruto':'Financiado',
    'valorLiberado':'Liberado',
    'valor_parcela':'Parcela',
    'prazo':'Prazo',
    'valorTotalComissao': 'Comissão'
})

df_crm['Data da Mensagem'] = (
    pd.to_datetime(df_crm['Data da Mensagem'], errors='coerce', utc=True)
    .dt.tz_localize(None)
    .dt.date
)

dados_filtrados = df_crm.copy()

dados_filtrados = dados_filtrados.drop_duplicates()

dados_filtrados['mensagens'] = dados_filtrados['Mensagem Inicial'].apply(mapeia_mensagens)

dados_filtrados['Financiado'] = dados_filtrados['Financiado'].astype(float).apply(formata_float)
dados_filtrados['Liberado'] = dados_filtrados['Liberado'].apply(formata_float)
dados_filtrados['Parcela'] = dados_filtrados['Parcela'].apply(formata_float)
dados_filtrados['Comissão'] = dados_filtrados['Comissão'].apply(formata_float)

##### ÁREA DO DASHBOARD #####

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE MENSAGENS INICIAIS #####
    mensagem_inicial = dados_filtrados['mensagens'].dropna().unique().tolist()
    mensagem_inicial = [str(x).strip() for x in mensagem_inicial if x is not None]
    mensagem_inicial = sorted(mensagem_inicial)
    
    if "filtro_mensagem" not in st.session_state:
        st.session_state.filtro_mensagem = []

    selectbox_mensagem = st.multiselect(
        'Selecione a Mensagem',
        mensagem_inicial,
        key="filtro_mensagem",
        placeholder='Selecionar'
    )

    if len(selectbox_mensagem) != 0:
        dados_filtrados['mensagens'] = dados_filtrados['mensagens'].astype(str).str.strip()
        filtros = [str(x).strip() for x in selectbox_mensagem]
        dados_filtrados = dados_filtrados[dados_filtrados['mensagens'].isin(filtros)]
        

    df_controle = dados_filtrados.copy()


    ##### FILTRO DE INTERVALO DE DATA CONSULTA #####
    dados_filtrados['Data da Liberação'] = (
        pd.to_datetime(dados_filtrados['Data da Liberação'], errors='coerce', utc=True)
        .dt.tz_localize(None)
        .dt.date
    )

    menor_data_liberacao, maior_data_liberacao = get_datas_liberacao(dados_filtrados)
    if "filtro_periodo_liberacao" not in st.session_state:
        st.session_state.filtro_periodo_liberacao = (menor_data_liberacao, date.today())

    if not pd.isnull(menor_data_liberacao):
        intervalo_liberacao = st.date_input(
            "Selecione a data da Liberação:",
            value=(),
            key="filtro_periodo_liberacao"
        )
        
        # Se o usuário selecionou apenas uma data, define fim como hoje
        if len(intervalo_liberacao) == 2:
            inicio_liberacao, fim_liberacao = intervalo_liberacao
            
        elif len(intervalo_liberacao) == 1:
            # Usuário selecionou apenas uma data
            inicio_liberacao = intervalo_liberacao[0]
            fim_liberacao = date.today()
            
        # Se o usuário não alterou o intervalo, mantém todas as linhas (inclusive NaT)
        try:

            if (inicio_liberacao, fim_liberacao) != (menor_data_liberacao, maior_data_liberacao):
                # Filtra as linhas de consulta dentro do intervalo
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['Data da Liberação'] >= inicio_liberacao) &
                    (dados_filtrados['Data da Liberação'] <= fim_liberacao)
                ]
                
        except:
            dados_filtrados = dados_filtrados[
                    dados_filtrados['Data da Liberação'].isna()
                ]

    # Botão de limpeza
    if st.button("🧹 Limpar filtros"):
        for key in list(st.session_state.keys()):
            if key.startswith("filtro_"):
                del st.session_state[key]
        st.rerun()


##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1a, col_2a = st.columns((2, 8))

    with col_1a:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2a:
        st.title(":blue[Análise das Campanhas]")

soma = (
    dados_filtrados['Liberado']
    .str.replace('.', '', regex=False)
    .str.replace(',', '.', regex=False)
    .astype(float)
    .sum()
)

soma_comissao = (
    dados_filtrados['Comissão']
    .str.replace('.', '', regex=False)
    .str.replace(',', '.', regex=False)
    .astype(float)
    .sum()
)

with st.container():
    col_1a, col_1b, col_1c = st.columns((2, 2, 6))
    
    ##### ÁREA DOS CARDS #####
    with col_1a:
        
        ##### CARD TOTAL LIBERADO #####
        metric_card("Total Liberado", f"R$ {soma:,.2f}".replace('.','|').replace(',','.').replace('|',','))

    with col_1b:
        ##### CARD TOTAL COMISSÃO #####
        metric_card("Total Comissão", f"R$ {soma_comissao:,.2f}".replace('.','|').replace(',','.').replace('|',','))


##### ÁREA DA TABELA #####
dados_filtrados['Data da Mensagem'] = pd.to_datetime(dados_filtrados['Data da Mensagem']).dt.strftime('%d/%m/%Y')
dados_filtrados['Data da Liberação'] = pd.to_datetime(dados_filtrados['Data da Liberação']).dt.strftime('%d/%m/%Y')

dados_filtrados = dados_filtrados[['Telefone','CPF','Nome','Data da Mensagem','Mensagem Inicial','Banco','Data da Liberação','Financiado','Liberado','Parcela','Prazo','Comissão']]

with st.container():
    st.dataframe(dados_filtrados, width='stretch', height=500, hide_index=True)

df_controle['Liberado'] = df_controle['Liberado'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
df_controle['Comissão'] = df_controle['Comissão'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)

controle = df_controle.groupby('mensagens').agg({'Telefone': 'count', 'Data da Liberação': 'count', 'Liberado': 'sum', 'Comissão': 'sum'}).reset_index()

controle = controle.rename(columns={'mensagens': 'Campanhas', 'Telefone': 'Volume', 'Data da Liberação': 'Digitados', 'Liberado': 'Valor de Produção', 'Comissão': 'Comissão Recebida'})
controle['Ticket Médio'] = controle['Valor de Produção'] / controle['Digitados']
controle['Investimento'] = 699.99
controle['CAC'] = controle['Investimento'] / controle['Digitados']
controle['ROI'] = (controle['Comissão Recebida'] - controle['Investimento']) / controle['Investimento']
controle['ROI'] = np.where(controle['ROI'] >= 0, ['🟢 +' + str(x) for x in controle['ROI']], ['🔴 ' + str(x) for x in controle['ROI']])

with st.container():
    st.dataframe(controle, width='stretch', height=500, hide_index=True)
