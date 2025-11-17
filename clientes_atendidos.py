import streamlit as st
import altair as alt
import pandas as pd
import numpy as np
import duckdb as dk
from datetime import date, datetime
from io import BytesIO
import io
import zipfile

from querys.querys_sql import QuerysSQL
from querys.connect import Conexao

pd.set_option('display.max_columns', None)


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Clientes Atendidos",
    page_icon="image/logo_agnus.ico",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

# ##### CRIAR INSTÂNCIA ÚNICA #####
consulta = QuerysSQL()

##### CACHE DE CONSULTAS #####
@st.cache_data(show_spinner=False)
def carregar_dados():
    conectar = Conexao()

    conectar.conectar_mysql()
    conectar.conectar_postgres()

    conn_mysql = conectar.obter_conexao_mysql()
    conn_postgres = conectar.obter_conexao_postgres()
    
    ########## CRM ##########
    ## Consulta CRM_Consulta
    consulta_crm_consulta = consulta.get_crm_consulta()
    df_crm_consulta1 = pd.read_sql(consulta_crm_consulta, conn_mysql)
    
    df_crm_consulta1['cpf'] = (
        df_crm_consulta1['cpf']
        .astype(str)
        .str.replace(r'\D', '', regex=True)  # remove tudo que não é dígito
        .str.strip()                         # remove espaços
    )
    
    df_crm_consulta1['cpf'] = df_crm_consulta1['cpf'].str.zfill(11)
    
    pega_clienteId = df_crm_consulta1.groupby('cpf')['clienteId'].first().reset_index()
    
    df_crm_consulta = pd.merge(df_crm_consulta1, pega_clienteId, on='cpf', how='left')
    
    df_crm_consulta = df_crm_consulta.loc[
        df_crm_consulta.groupby('cpf')['dataConsulta'].idxmax()
    ]
    
    ## Consulta CRM_Tabela
    consulta_crm_tabela = consulta.get_crm_tabela()
    df_crm_tabela = pd.read_sql(consulta_crm_tabela, conn_mysql)
    
    ## Consulta CRM_Parcela
    consulta_crm_parcela = consulta.get_crm_parcela()
    df_crm_parcela = pd.read_sql(consulta_crm_parcela, conn_mysql)

    ## Ajuste de tipos e realiza os JOIN's
    df_crm_consulta['tabelaId'] = df_crm_consulta['tabelaId'].astype('Int64')
    df_crm_tabela['tabelaId'] = df_crm_tabela['tabelaId'].astype(int)

    df_crm1 = pd.merge(df_crm_consulta, df_crm_parcela, on='consultaId', how='left')
    
    df_crm2 = pd.merge(df_crm1, df_crm_tabela, on='tabelaId', how='left')
    
    ## Consulta CRM_Cliente
    consulta_crm_cliente = consulta.get_crm_cliente()
    df_crm_cliente = pd.read_sql(consulta_crm_cliente, conn_mysql)
    
    df_crm_cliente['cpf'] = (
        df_crm_cliente['cpf']
        .astype(str)
        .str.replace(r'\D', '', regex=True)  # remove tudo que não é dígito
        .str.strip()                         # remove espaços
    )
    df_crm_cliente['cpf'] = df_crm_cliente['cpf'].str.zfill(11)
    
    ## Consulta CRM_Telefone
    consulta_crm_telefone = consulta.get_crm_telefone()
    df_crm_telefone = pd.read_sql(consulta_crm_telefone, conn_mysql)
    
    ## Consulta CRM_Lead
    consulta_crm_lead = consulta.get_crm_lead()
    df_crm_lead = pd.read_sql(consulta_crm_lead, conn_mysql)
    
    df_crm3 = pd.merge(df_crm2, df_crm_lead, on='consultaId', how='outer')
    df_crm3['nome'] = None
    df_crm3['telefone_crm'] = None
    df_crm3['erros'] = df_crm3['erros'].fillna('Sucesso')
    df_crm3 = df_crm3[['cpf', 'clienteId', 'telefone_lead', 'nome', 'telefone_crm', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela']]
    
    df_crm4 = pd.merge(df_crm_cliente, df_crm_telefone, on='clienteId', how='outer')
    
    df_crm5 = pd.merge(df_crm4, df_crm_lead, on='clienteId', how='outer')
    df_crm5['dataConsulta'] = None
    df_crm5['erros'] = None
    df_crm5['tabelaId'] = None
    df_crm5['valorLiberado'] = None
    df_crm5['valorContrato'] = None
    df_crm5['parcelas'] = None
    df_crm5['tabela'] = None
    df_crm5 = df_crm5[['cpf', 'clienteId', 'telefone_lead', 'nome', 'telefone_crm', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela']]
    
    df_crm  = pd.concat([df_crm3, df_crm5], ignore_index=True)
    df_crm['dataConsulta'] = pd.to_datetime(df_crm['dataConsulta']).dt.date

    df_crm['telefone_aux1'] = df_crm['telefone_crm'].replace('', np.nan).fillna(df_crm['telefone_lead'])
    
    df_crm = df_crm[['cpf', 'nome', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'telefone_aux1']]
    #########################

    ########## DIGISAC ##########
    consulta_digisac = consulta.get_digisac()
    df_digisac = pd.read_sql_query(consulta_digisac, conn_postgres)
    df_digisac['data_digisac'] = pd.to_datetime(df_digisac['data']).dt.date

    df_digisac['telefone_digisac'] = df_digisac['telefone_digisac'].astype(str).str.replace(r'^(55)(?=\d{11,})', '', regex=True)
    df_digisac['telefone_digisac'] = df_digisac['telefone_digisac'].astype(str).apply(
        lambda x: x[:2] + '9' + x[2:] if len(x) <= 10 and x.isdigit() else x
    )

    df_digisac = df_digisac[['cpf_digisac', 'nome_interno', 'telefone_digisac', 'falha', 'data_digisac']]
    #############################
    
    ########## DF1 = CRM <- DIGISAC ##########
    # parte1 = pd.merge(df4, df_digisac[(df_digisac['cpf_digisac'].notna()) | (df_digisac['cpf_digisac'].notnull())], left_on='cpf', right_on='cpf_digisac', how='outer')
    # parte2 = pd.merge(df4, df_digisac[(df_digisac['cpf_digisac'].isna()) | (df_digisac['cpf_digisac'].isnull())], left_on='col_aux5', right_on='telefone_digisac', how='outer')
    
    # df = pd.concat([parte1, parte2], ignore_index=True)

    # df = pd.merge(df1, df_digisac, left_on=['col_aux5'], right_on=['telefone_digisac'], how='outer')
    df1 = pd.merge(df_crm, df_digisac, left_on=['cpf'], right_on=['cpf_digisac'], how='outer')

    df1['cpf_aux1'] = df1['cpf'].replace('', np.nan).fillna(df1['cpf_digisac'])
    df1.loc[df1['cpf_aux1'].str.contains('opt|\[\]', case=False, na=False), 'cpf_aux1'] = None
    df1['nome_aux1'] = df1['nome'].replace('', np.nan).fillna(df1['nome_interno'])
    df1['telefone_aux2'] = df1['telefone_aux1'].replace('', np.nan).fillna(df1['telefone_digisac'])
    
    df1 = df1[['cpf_aux1', 'nome_aux1', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'telefone_aux2', 'falha', 'data_digisac']]
    ##########################################
    
    ########## CORBAN ##########
    consulta_corban = consulta.get_corban()
    df_corban = pd.read_sql_query(consulta_corban, conn_postgres)
    df_corban['cpf_corban'] = df_corban['cpf_corban'].astype(str).str.zfill(11)
    df_corban['data_atualizacao_api'] = pd.to_datetime(df_corban['data_atualizacao_api']).dt.date

    df_corban = df_corban[['cpf_corban', 'nome_corban', 'telefone_propostas', 'status_api', 'data_atualizacao_api']]
    ############################

    ########## DF2 = DF1 <- CORBAN ##########
    df2 = pd.merge(df1, df_corban, left_on=['cpf_aux1'], right_on=['cpf_corban'], how='outer')
    
    df2['cpf_aux2'] = df2['cpf_aux1'].replace('', np.nan).fillna(df2['cpf_corban'])
    df2['nome_aux2'] = df2['nome_aux1'].replace('', np.nan).fillna(df2['nome_corban'])
    df2['telefone_aux3'] = df2['telefone_aux2'].replace('', np.nan).fillna(df2['telefone_propostas'])

    df2 = df2[['cpf_aux2', 'nome_aux2', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'telefone_aux3', 'falha', 'data_digisac', 'status_api', 'data_atualizacao_api']]
    ########################################
    
    ########## TELEFONES CORBAN ##########
    consulta_telefone_corban = consulta.get_telefones_corban()
    df_telefones_corban = pd.read_sql_query(consulta_telefone_corban, conn_postgres)
    df_telefones_corban['cpf_telefone_corban'] = df_telefones_corban['cpf_telefone_corban'].astype(str).str.zfill(11)
    ######################################
    
    ########## DF3 = DF2 <- TELEFONES CORBAN ##########
    df3 = pd.merge(df2, df_telefones_corban, left_on=['cpf_aux2'], right_on=['cpf_telefone_corban'], how='outer')
    
    df3['cpf_aux3'] = df3['cpf_aux2'].replace('', np.nan).fillna(df3['cpf_telefone_corban'])
    df3['telefone_aux4'] = df3['telefone_aux3'].replace('', np.nan).fillna(df3['telefone_corban'])

    df3 = df3[['cpf_aux3', 'nome_aux2', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'telefone_aux4', 'falha', 'data_digisac', 'status_api', 'data_atualizacao_api']]
    ########################################
    
    ########## DISPAROS ##########
    consulta_disparos = consulta.disparos()
    df_disparos = pd.read_sql_query(consulta_disparos, conn_postgres)
    df_disparos['cpf_disparos'] = df_disparos['cpf_disparos'].astype(str).str.zfill(11)
    ##############################

    ########## DF4 = DF3 <- DISPAROS ##########
    df4 = pd.merge(df3, df_disparos, left_on=['cpf_aux3'], right_on=['cpf_disparos'], how='outer')
    
    df4['cpf_aux4'] = df4['cpf_aux3'].replace('', np.nan).fillna(df4['cpf_disparos'])
    df4['telefone_aux5'] = df4['telefone_aux4'].replace('', np.nan).fillna(df4['telefone_disparos'])

    df4 = df4[['cpf_aux4', 'nome_aux2', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'telefone_aux5', 'falha', 'data_digisac', 'status_api', 'data_atualizacao_api']]
    ########################################
    
    ########## BASES CONSOLIDADAS ##########
    consulta_base_consolidada = consulta.get_base_consolidada()
    df_base_consolidada = pd.read_sql_query(consulta_base_consolidada, conn_postgres)
    df_base_consolidada['cpf_consolidado'] = df_base_consolidada['cpf_consolidado'].astype(str).str.zfill(11)
    df_base_consolidada['telefone_consolidado'] = df_base_consolidada['telefone_consolidado'].astype(str).str.replace(r'\.0', '', regex=True)
    ########################################

    ########## DF5 = DF4 <- BASES CONSOLIDADAS ##########
    df = pd.merge(df4, df_base_consolidada, left_on=['cpf_aux4'], right_on=['cpf_consolidado'], how='outer')
    
    df['cpf_aux5'] = df['cpf_aux4'].replace('', np.nan).fillna(df['cpf_consolidado'])
    df['nome_aux3'] = df['nome_aux2'].replace('', np.nan).fillna(df['nome_consolidado'])
    df['telefone_aux6'] = df['telefone_aux5'].replace('', np.nan).fillna(df['telefone_consolidado'])

    df = df[['dataConsulta', 'cpf_aux5', 'nome_aux3', 'telefone_aux6', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'data_digisac', 'falha', 'data_atualizacao_api', 'status_api']]
    ########################################
    
    # ########## DF1 = CRM <- TELEFONES CORBAN ##########
    # df1 = pd.merge(df_crm, df_telefones_corban, left_on=['cpf'], right_on=['cpf_telefone_corban'], how='outer')
    
    # df1['col_aux2'] = np.where(df1['col_aux1'].notna() & df1['col_aux1'].notnull() & (df1['col_aux1'] != ''), df1['col_aux1'], df1['telefone_corban'])

    # df1['col_aux2'] = df1['col_aux2'].astype(str).str.replace(r'^(55)(?=\d{11,})', '', regex=True)
    # df1['col_aux2'] = df1['col_aux2'].astype(str).apply(
    #     lambda x: x[:2] + '9' + x[2:] if len(x) <= 10 and x.isdigit() else x
    # )

    # df1 = df1[['cpf', 'nome', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'col_aux2']]
    # #############################################
    
    # ########## DF3 = DF2 <- DISPAROS ##########
    # df3 = pd.merge(df2, df_disparos, left_on=['cpf'], right_on=['cpf_disparos'], how='outer')
   
    # df3['col_aux4'] = np.where(df3['col_aux3'].notna() & df3['col_aux3'].notnull() & (df3['col_aux3'] != ''), df3['col_aux3'], df3['telefone_disparos'])
    
    # df3 = df3[['cpf', 'nome', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'col_aux4', 'data_atualizacao_api', 'status_api']]
    # ########################################

    # ########## DF4 = DF3 <- CONSOLIDADOS ##########
    # df4 = pd.merge(df3, df_base_consolidada, left_on=['cpf'], right_on=['cpf_consolidado'], how='outer')
   
    # df4['col_aux5'] = np.where(df4['col_aux4'].notna() & df4['col_aux4'].notnull() & (df4['col_aux4'] != ''), df4['col_aux4'], df4['telefone_consolidado'])

    # df4['nome_aux1'] = np.where(df4['nome'].notna() & df4['nome'].notnull() & (df4['nome'] != ''), df4['nome'], df4['nome_consolidado'])

    # df4 = df4[['dataConsulta', 'cpf', 'nome_aux1', 'erros', 'tabela', 'parcelas', 'valorLiberado', 'valorContrato', 'col_aux5', 'data_atualizacao_api', 'status_api']]
    # ########################################
    
    # df = df[['dataConsulta', 'cpf', 'nome_aux1', 'telefone', 'erros', 'tabela', 'parcelas', 'valorLiberado', 'valorContrato', 'data', 'falha', 'data_atualizacao_api', 'status_api']]
    
    # Condição: data_inicio <= data_fim
    cond = df['dataConsulta'] <= df['data_atualizacao_api']
    
    # Aplicando a regra
    df.loc[~cond, ['status_api', 'data_atualizacao_api']] = None
    
    renomear = {
                    'dataConsulta': 'Data Consulta',
                    'cpf_aux5': 'CPF',
                    'nome_aux3': 'Nome',
                    'telefone_aux6': 'Telefone',
                    'erros': 'Retorno Consulta',
                    'tabela': 'Tabelas',
                    'data_digisac': 'Data Disparo',
                    'parcelas': 'Parcelas',
                    'valorLiberado': 'Valor Liberado',
                    'valorContrato': 'Valor Contrato',
                    'falha': 'Retorno Digisac',
                    'data_atualizacao_api': 'Data Corban',
                    'status_api': 'Status Corban'
                }
    
    df = df.rename(columns=renomear)
    
    # Remove 55 dos telefones
    df['Telefone'] = df['Telefone'].astype(str).str.replace(r'^(55)(?=\d{11,})', '', regex=True)

    # Adiciona "9" depois do segundo dígito, se o número tiver apenas 10 dígitos (ex: DDD + 8 dígitos)
    df['Telefone'] = df['Telefone'].astype(str).apply(
        lambda x: x[:2] + '9' + x[2:] if len(x) <= 10 and x.isdigit() else x
    )
    
    df['Telefone'] = df['Telefone'].replace('nan', None)
    
    df = (
        df[~((df['Data Consulta'].isna() & df['Retorno Consulta'].isna()) & df.duplicated(subset='CPF', keep=False))]
        .drop_duplicates(subset='CPF', keep='first')
    )
    
    return df.drop_duplicates()
    

##### CARREGAR OS DADOS (1x) #####
dados = carregar_dados()


##### FUNÇÃO PARA OBTER AS DATAS CONSULTA #####
def get_datas_consulta(dados):
    # Remove linhas com Data Consulta vazia
    dados = dados.dropna(subset=['Data Consulta'])

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = dados['Data Consulta'].min()
    maior_data = date.today()
    
    return menor_data, maior_data

##### FUNÇÃO PARA OBTER AS DATAS DISPAROS #####
def get_datas_disparos(dados):
    # Remove linhas com Data disparos vazia
    dados = dados.dropna(subset=['Data Disparo'])

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = dados['Data Disparo'].min()
    maior_data = date.today()

    return menor_data, maior_data

##### FUNÇÃO PARA OBTER AS DATAS CORBAN #####
def get_datas_corban(dados):
    # Remove linhas com Data Corban vazia
    dados = dados.dropna(subset=['Data Corban'])

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = dados['Data Corban'].min()
    maior_data = date.today()

    return menor_data, maior_data

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


##### ÁREA DO DASHBOARD #####

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE CPF #####
    if "filtro_cpf" not in st.session_state:
        st.session_state.filtro_cpf = 'Todos'

    lista_cpf = ["Todos", "Sem CPF", "Com CPF"]

    selectbox_cpf = st.selectbox(
        'Selecione COM ou SEM CPF',
        lista_cpf,
        key="filtro_cpf"
    )

    ##### FILTRO DE TELEFONES #####
    if "filtro_telefone" not in st.session_state:
        st.session_state.filtro_telefone = 'Todos'

    lista_telefone = ["Todos", "Sem Telefone", "Com Telefone"]

    selectbox_telefone = st.selectbox(
        'Selecione COM ou SEM Telefone',
        lista_telefone,
        key="filtro_telefone",
        placeholder='Selecionar'
    )

    ##### FILTRO DE ERROS CONSULTA #####
    erros_consulta = dados['Retorno Consulta'].dropna().unique().tolist()
    erros_consulta = [str(x).strip() for x in erros_consulta if x is not None]
    erros_consulta = sorted(erros_consulta)

    if "filtro_erros_consulta" not in st.session_state:
        st.session_state.filtro_erros_consulta = []
    
    selectbox_erros_consulta = st.multiselect(
        'Selecione os Retornos Consulta',
        erros_consulta,
        key="filtro_erros_consulta",
        placeholder='Selecionar'
    )

    ##### FILTRO DE ERROS DIGISAC #####
    erros_digisac = dados['Retorno Digisac'].dropna().unique().tolist()
    erros_digisac = sorted(erros_digisac)

    if "filtro_erros_digisac" not in st.session_state:
        st.session_state.filtro_erros_digisac = []

    selectbox_erros_digisac = st.multiselect(
        'Selecione os Retornos Digisac',
        erros_digisac,
        key="filtro_erros_digisac",
        placeholder = 'Selecionar'
    )

    ##### FILTRO DE STATUS CORBAN #####
    status_corban = dados['Status Corban'].dropna().unique().tolist()
    status_corban = sorted(status_corban)

    if "filtro_status_corban" not in st.session_state:
        st.session_state.filtro_status_corban = []

    selectbox_status_corban = st.multiselect(
        'Selecione os Status Corban',
        status_corban,
        key="filtro_status_corban",
        placeholder='Selecionar'
    )

    dados_filtrados = dados.copy()

    # CPF
    if selectbox_cpf == 'Sem CPF':
        dados_filtrados = dados_filtrados[dados_filtrados['CPF'].isna()]
    elif selectbox_cpf == 'Com CPF':
        dados_filtrados = dados_filtrados[dados_filtrados['CPF'].notna()]

    # Telefone
    if selectbox_telefone == 'Sem Telefone':
        dados_filtrados = dados_filtrados[dados_filtrados['Telefone'].isna()]
    elif selectbox_telefone == 'Com Telefone':
        dados_filtrados = dados_filtrados[dados_filtrados['Telefone'].notna()]

    # Retorno Consulta
    if len(selectbox_erros_consulta) != 0:
        dados['Retorno Consulta'] = dados['Retorno Consulta'].astype(str).str.strip()
        filtros = [str(x).strip() for x in selectbox_erros_consulta]
        dados_filtrados = dados_filtrados[dados_filtrados['Retorno Consulta'].isin(filtros)]

    # Retorno Digisac
    if len(selectbox_erros_digisac) != 0:
        dados_filtrados = dados_filtrados[dados_filtrados['Retorno Digisac'].isin(selectbox_erros_digisac)]

    # Status Corban
    if len(selectbox_status_corban) != 0:
        dados_filtrados = dados_filtrados[dados_filtrados['Status Corban'].isin(selectbox_status_corban)]

    ##### FILTRO DE INTERVALO DE DATA CONSULTA #####
    menor_data_consulta, maior_data_consulta = get_datas_consulta(dados)
    if "filtro_periodo_consulta" not in st.session_state:
        st.session_state.filtro_periodo_consulta = (menor_data_consulta, date.today())

    if not pd.isnull(menor_data_consulta):
        intervalo_consulta = st.date_input(
            "Selecione a data da Consulta:",
            value=(),
            key="filtro_periodo_consulta"
        )
        
        # Se o usuário selecionou apenas uma data, define fim como hoje
        if len(intervalo_consulta) == 2:
            inicio_consulta, fim_consulta = intervalo_consulta
            
        elif len(intervalo_consulta) == 1:
            # Usuário selecionou apenas uma data
            inicio_consulta = intervalo_consulta[0]
            fim_consulta = date.today()
            
        # Se o usuário não alterou o intervalo, mantém todas as linhas (inclusive NaT)
        try:

            if (inicio_consulta, fim_consulta) != (menor_data_consulta, maior_data_consulta):
                # Filtra as linhas de consulta dentro do intervalo
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['Data Consulta'] >= inicio_consulta) &
                    (dados_filtrados['Data Consulta'] <= fim_consulta)
                ]
                
        except:
            dados_filtrados = dados_filtrados[
                    dados_filtrados['Data Consulta'].isna()
                ]
            
    ##### FILTRO DE INTERVALO DE DATA DISPAROS #####
    menor_data_disparos, maior_data_disparos = get_datas_disparos(dados)
    
    if "filtro_periodo_disparos" not in st.session_state:
        st.session_state.filtro_periodo_disparos = (menor_data_disparos, maior_data_disparos)

    if not pd.isnull(menor_data_disparos):
        intervalo_disparos = st.date_input(
            "Selecione a data do disparos:",
            value=(),
            key="filtro_periodo_disparos"
        )
        
        # Se o usuário selecionou apenas uma data, define fim como hoje
        if len(intervalo_disparos) == 2:
            inicio_disparos, fim_disparos = intervalo_disparos
        elif len(intervalo_disparos) == 1:
            # Usuário selecionou apenas uma data
            inicio_disparos = intervalo_disparos[0]
            fim_disparos = date.today()
        
        try:
            if (inicio_disparos, fim_disparos) != (menor_data_disparos, maior_data_disparos):
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['Data Disparo'] >= inicio_disparos) &
                    (dados_filtrados['Data Disparo'] <= fim_disparos)
                ]
            else:
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['Data Disparo'] >= inicio_disparos) &
                    (dados_filtrados['Data Disparo'] <= fim_disparos) |
                    (dados_filtrados['Data Disparo'].isna())
                ]
        except:
            dados_filtrados = dados_filtrados[
                    (dados_filtrados['Data Disparo'].isna()) |
                    (dados_filtrados['Data Disparo'].isnull())
                ]

    ##### FILTRO DE INTERVALO DE DATA CORBAN #####
    menor_data_corban, maior_data_corban = get_datas_corban(dados)
    
    if "filtro_periodo_corban" not in st.session_state:
        st.session_state.filtro_periodo_corban = (menor_data_corban, maior_data_corban)

    if not pd.isnull(menor_data_corban):
        intervalo_corban = st.date_input(
            "Selecione a data do Corban:",
            value=(),
            key="filtro_periodo_corban"
        )
        
        # Se o usuário selecionou apenas uma data, define fim como hoje
        if len(intervalo_corban) == 2:
            inicio_corban, fim_corban = intervalo_corban
        elif len(intervalo_corban) == 1:
            # Usuário selecionou apenas uma data
            inicio_corban = intervalo_corban[0]
            fim_corban = date.today()
        
        try:
            if (inicio_corban, fim_corban) != (menor_data_corban, maior_data_corban):
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['Data Corban'] >= inicio_corban) &
                    (dados_filtrados['Data Corban'] <= fim_corban)
                ]
            else:
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['Data Corban'] >= inicio_corban) &
                    (dados_filtrados['Data Corban'] <= fim_corban) |
                    (dados_filtrados['Data Corban'].isna())
                ]
        except:
            dados_filtrados = dados_filtrados[
                    (dados_filtrados['Data Corban'].isna()) |
                    (dados_filtrados['Data Corban'].isnull())
                ]

    # Botão de limpeza
    if st.button("🧹 Limpar filtros"):
        st.session_state.clear()
        st.rerun()

dados_filtrados['Data Consulta'] = pd.to_datetime(dados_filtrados['Data Consulta'], errors='coerce')
dados_filtrados['Data Disparo'] = pd.to_datetime(dados_filtrados['Data Disparo'], errors='coerce')
dados_filtrados['Data Corban'] = pd.to_datetime(dados_filtrados['Data Corban'], errors='coerce')

dados_filtrados['Data Consulta'] = dados_filtrados['Data Consulta'].dt.strftime('%d/%m/%Y')
dados_filtrados['Data Disparo'] = dados_filtrados['Data Disparo'].dt.strftime('%d/%m/%Y')
dados_filtrados['Data Corban'] = dados_filtrados['Data Corban'].dt.strftime('%d/%m/%Y')

dados_filtrados['Telefone'] = dados_filtrados['Telefone'].astype(str).apply(
        lambda x: x[:2] + '9' + x[2:] if len(x) <= 10 and x.isdigit() else x
    )

dados_csv = dados_filtrados[['Nome','Telefone']]
dados_csv = dados_csv[~dados_csv['Telefone'].isnull()].drop_duplicates(subset='Telefone')
dados_csv['Telefone'] = '55' + dados_csv['Telefone'].astype(str)

##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1a, col_2a = st.columns((2, 8))

    with col_1a:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2a:
        st.title(":blue[Análise dos Clientes]")

with st.container():
    col_1a, col_1b, col_1c = st.columns((2, 2, 6))
    
    ##### ÁREA DOS CARDS #####
    with col_1a:
        
        ##### CARD TOTAL DE CONSULTAS #####
        metric_card("Consultas Realizadas", f"{format(int(len(dados_filtrados[(dados_filtrados['Data Consulta'].notna()) & (dados_filtrados['Data Consulta'].notnull())])), ',').replace(',', '.')}")

    with col_1b:
        ##### CARD TOTAL VISUALIZAÇÕES #####
        metric_card("Visualizações na Tela", f"{format(int(len(dados_filtrados)), ',').replace(',', '.')}")


##### ÁREA DA TABELA #####
with st.container():
    st.dataframe(dados_filtrados, width='stretch', height=500, hide_index=True)

with st.container():
    col_1, col_2, col_3, col_4 = st.columns((1,1,1,7))
    
    with col_1:
        if len(dados_csv) > 0:
            visibilidade = False
            valor_minimo = 1
        else:
            visibilidade = True   
            valor_minimo = 0 
        qtd = st.number_input("Linhas por arquivo", min_value=valor_minimo, value=len(dados_csv), step=100, disabled=visibilidade)
        # st.text_input(label='', value=format(int(len(dados_csv)), ',').replace(',', '.'), disabled=False)
    
    with col_2:
        ##### BOTÃO EXPORTAR TABELA #####
        
        buffer_zip = io.BytesIO()
        try:
            partes = [dados_csv[i:i + qtd] for i in range(0, len(dados_csv), qtd)]
        except:
            partes = [dados_csv]


        # Cria um ZIP em memória
        with zipfile.ZipFile(buffer_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
            for idx, parte in enumerate(partes, start=1):
                csv_bytes = parte.to_csv(index=False, encoding="utf-8", sep=";").encode("utf-8")
                zipf.writestr(f"parte_{idx}.csv", csv_bytes)

        buffer_zip.seek(0)

        st.write('')
        st.write('')
        
        # Um único botão que gera e baixa
        st.download_button(
            label="⬇️ Baixar Digisac",
            data=buffer_zip,
            file_name="arquivos_divididos.zip",
            mime="application/zip",
            disabled=visibilidade
        )
        # csv = dados_csv.to_csv(index=False).encode("utf-8")
       
        # # --- Botão para download ---
        # st.write('')
        # st.write('')
        # st.download_button(
        #     label="⬇️ Baixar Digisac",
        #     data=csv,
        #     file_name="consultas.csv",
        #     mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        # )

    with col_3:
        qtd_total = len(dados_filtrados)
        if qtd_total > 0:
            visibilidade_total = False
        else:
            visibilidade_total = True

        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            dados_filtrados.to_excel(writer, index=False, sheet_name='Visualização')
        buffer.seek(0)  # volta o ponteiro para o início


        # --- Botão para download ---
        st.write('')
        st.write('')
        st.download_button(
            label="⬇️ Baixar Visualização",
            data=buffer,
            file_name="visualizacoes.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            disabled=visibilidade_total
        )

    st.write(f'Sem Nome: {len(dados_filtrados[dados_filtrados["Nome"].isnull()])}')
        