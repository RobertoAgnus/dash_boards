import streamlit as st
import altair as alt
import pandas as pd
import numpy as np
from datetime import date, datetime
from io import BytesIO

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
    consulta_crm_consulta = consulta.get_crm_consulta()
    df_crm_consulta1 = pd.read_sql(consulta_crm_consulta, conn_mysql)
    df_crm_consulta1['cpf'] = (
        df_crm_consulta1['cpf']
        .astype(str)
        .str.replace(r'\D', '', regex=True)  # remove tudo que não é dígito
        .str.strip()                         # remove espaços
    )
    # df_crm_consulta1 = df_crm_consulta1[(~df_crm_consulta1['cpf'].isna()) & (~df_crm_consulta1['cpf'].isnull()) & (df_crm_consulta1['cpf'] != '')]

    df_crm_consulta1['cpf'] = df_crm_consulta1['cpf'].str.zfill(11)
    df_crm_consulta1.loc[~df_crm_consulta1['cpf'].str.fullmatch(r'\d{11}'), 'cpf'] = None
    df_crm_consulta = df_crm_consulta1.loc[df_crm_consulta1.groupby('cpf')['dataConsulta'].idxmax()]

    consulta_crm_cliente = consulta.get_crm_cliente()
    df_crm_cliente = pd.read_sql(consulta_crm_cliente, conn_mysql)
    
    df_crm_cliente['cpf'] = (
        df_crm_cliente['cpf']
        .astype(str)
        .str.replace(r'\D', '', regex=True)  # remove tudo que não é dígito
        .str.strip()                         # remove espaços
    )
    df_crm_cliente['cpf'] = df_crm_cliente['cpf'].str.zfill(11)
    df_crm_cliente.loc[~df_crm_cliente['cpf'].str.fullmatch(r'\d{11}'), 'cpf'] = None
    # df_crm_cliente.to_csv('C:/Users/User/Downloads/teste_clientes.csv', index=False)

    consulta_crm_telefone = consulta.get_crm_telefone()
    df_crm_telefone = pd.read_sql(consulta_crm_telefone, conn_mysql)

    consulta_crm_lead = consulta.get_crm_lead()
    df_crm_lead = pd.read_sql(consulta_crm_lead, conn_mysql)

    consulta_crm_tabela = consulta.get_crm_tabela()
    df_crm_tabela = pd.read_sql(consulta_crm_tabela, conn_mysql)

    consulta_crm_parcela = consulta.get_crm_parcela()
    df_crm_parcela = pd.read_sql(consulta_crm_parcela, conn_mysql)

    df_crm1 = pd.merge(df_crm_consulta, df_crm_parcela, on='consultaId', how='left')

    df_crm1['tabelaId'] = pd.to_numeric(df_crm1['tabelaId'], errors='coerce')
    df_crm_tabela['tabelaId'] = pd.to_numeric(df_crm_tabela['tabelaId'], errors='coerce')

    df_crm2 = pd.merge(df_crm1, df_crm_tabela, on='tabelaId', how='left')
    df_crm3 = pd.merge(df_crm2, df_crm_lead, on='consultaId', how='left')
    df_crm3['nome'] = None
    df_crm3['telefone'] = None
    df_crm3 = df_crm3[['consultaId', 'cpf', 'clienteId', 'telefoneLead', 'nome', 'telefone', 'dataConsulta', 'erros', 'tabelaId', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela']]
    df_crm3['erros'] = df_crm3['erros'].fillna('Sucesso')
    # df_crm3.to_csv('C:/Users/User/Downloads/teste_consulta.csv', index=False)

    df_crm4 = pd.merge(df_crm_cliente, df_crm_telefone, on='clienteId', how='left')
    df_crm5 = pd.merge(df_crm4, df_crm_lead, on='clienteId', how='left')
    df_crm5['dataConsulta'] = None
    df_crm5['erros'] = None
    df_crm5['tabelaId'] = None
    df_crm5['valorLiberado'] = None
    df_crm5['valorContrato'] = None
    df_crm5['parcelas'] = None
    df_crm5['tabela'] = None
    df_crm5 = df_crm5[['consultaId', 'cpf', 'clienteId', 'telefoneLead', 'nome', 'telefone', 'dataConsulta', 'erros', 'tabelaId', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela']]
    
    df_crm  = pd.concat([df_crm3, df_crm5], ignore_index=True)

    # mantém só CPFs com 11 dígitos
    df_crm['dataConsulta'] = pd.to_datetime(df_crm['dataConsulta']).dt.date

    df_crm['col_aux1'] = np.where(df_crm['telefone'].notna() & df_crm['telefone'].notnull() & (df_crm['telefone'] != ''), df_crm['telefone'], df_crm['telefoneLead'])

    df_crm['dataConsulta'] = pd.to_datetime(df_crm['dataConsulta'])
    #########################

    ########## DIGISAC ##########
    consulta_digisac = consulta.get_digisac()
    df_digisac = pd.read_sql_query(consulta_digisac, conn_postgres)
    df_digisac['data_digisac'] = pd.to_datetime(df_digisac['data'])
    #############################
    
    ########## TELEFONES CORBAN ##########
    consulta_telefone_corban = consulta.get_telefones_corban()
    df_telefones_corban = pd.read_sql_query(consulta_telefone_corban, conn_postgres)
    df_telefones_corban['cpf'] = df_telefones_corban['cpf'].astype(str).str.zfill(11)
    ######################################
    
    ########## CORBAN ##########
    consulta_corban = consulta.get_corban()
    df_corban = pd.read_sql_query(consulta_corban, conn_postgres)
    df_corban['cpf'] = df_corban['cpf'].astype(str).str.zfill(11)
    df_corban['data_atualizacao_api'] = pd.to_datetime(df_corban['data_atualizacao_api']).dt.date
    df_corban['data_api'] = pd.to_datetime(df_corban['data_atualizacao_api'])
    ############################

    ########## DF1 = CRM <- TELEFONES CORBAN ##########
    df1 = pd.merge(df_crm, df_telefones_corban, left_on=['cpf'], right_on=['cpf'], how='left')

    df1['col_aux2'] = np.where(df1['col_aux1'].notna() & df1['col_aux1'].notnull() & (df1['col_aux1'] != ''), df1['col_aux1'], df1['telefoneCorban'])
    #############################################
    
    ########## DF2 = DF1 <- DIGISAC ##########
    df2 = pd.merge(df1, df_digisac, left_on=['cpf'], right_on=['cpf'], how='left')
    
    df2['col_aux3'] = np.where(df2['col_aux2'].notna() & df2['col_aux2'].notnull() & (df2['col_aux2'] != ''), df2['col_aux2'], df2['telefone_y'])
    ##########################################

    ########## DF = DF2 <- CORBAN ##########
    df = pd.merge(df2, df_corban, left_on=['cpf'], right_on=['cpf'], how='left')
    
    df['telefone'] = np.where(df['col_aux3'].notna() & df['col_aux3'].notnull() & (df['col_aux3'] != ''), df['col_aux3'], df['telefonePropostas'])
    ########################################
    
    df = df[['dataConsulta', 'cpf', 'nome', 'telefone', 'erros', 'tabela', 'parcelas', 'valorLiberado', 'valorContrato', 'data', 'falha', 'data_atualizacao_api', 'status_api']]
    
    # Condição: data_inicio <= data_fim
    cond = df['dataConsulta'] <= df['data_atualizacao_api']
    
    # Aplicando a regra
    df.loc[~cond, ['status_api', 'data_atualizacao_api']] = None
    
    renomear = {
                    'dataConsulta': 'Data Consulta',
                    'cpf': 'CPF',
                    'nome': 'Nome',
                    'telefone': 'Telefone',
                    'erros': 'Retorno Consulta',
                    'tabela': 'Tabelas',
                    'data': 'Data Disparo',
                    'parcelas': 'Parcelas',
                    'valorLiberado': 'Valor Liberado',
                    'valorContrato': 'Valor Contrato',
                    'falha': 'Retorno Digisac',
                    'data_atualizacao_api': 'Data Corban',
                    'status_api': 'Status Corban'
                }
    
    df = df.rename(columns=renomear)
        # Adiciona "9" depois do segundo dígito, se o número tiver apenas 10 dígitos (ex: DDD + 8 dígitos)
    df['Telefone'] = df['Telefone'].astype(str).apply(
        lambda x: x[:2] + '9' + x[2:] if len(x) <= 10 and x.isdigit() else x
    )
    
    df['Telefone'] = df['Telefone'].replace('nan', None)
    
    return df.drop_duplicates()
    

##### CARREGAR OS DADOS (1x) #####
dados = carregar_dados()


##### FUNÇÃO PARA OBTER AS DATAS CONSULTA #####
def get_datas_consulta(dados):
    df_datas = dados['Data Consulta']
    
    # Converting the 'data' column to datetime format (caso não esteja)
    df_datas['Data Consulta'] = pd.to_datetime(df_datas, dayfirst=True)

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = df_datas['Data Consulta'].min()
    maior_data = date.today()

    return menor_data, maior_data

##### FUNÇÃO PARA OBTER AS DATAS CORBAN #####
def get_datas_corban(dados):
    df_datas = dados['Data Corban']
    
    # Converting the 'data' column to datetime format (caso não esteja)
    df_datas['Data Corban'] = pd.to_datetime(df_datas, dayfirst=True)

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = df_datas['Data Corban'].min()
    maior_data = date.today()

    if ~pd.isna(menor_data):
        menor_data = menor_data.date()
    
    return menor_data, maior_data

def filtrar_data(intervalo, dados, flag):
    if len(intervalo) == 2:
        data_inicio, data_fim = intervalo
    
    elif len(intervalo) == 1:
        data_inicio = intervalo[0]
        data_fim = datetime.strptime('2030-12-31', "%Y-%m-%d")
    
    else:
        if flag == 'consulta':
            data_inicio, data_fim = get_datas_consulta(dados)
        else:
            data_inicio, data_fim = get_datas_corban(dados)

    data_inicio = pd.to_datetime(data_inicio)
    data_fim = pd.to_datetime(data_fim)

    return (data_inicio, data_fim)

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
        dados_filtrados = dados[dados['Retorno Consulta'].isin(filtros)]

    # Retorno Digisac
    if len(selectbox_erros_digisac) != 0:
        dados_filtrados = dados_filtrados[dados_filtrados['Retorno Digisac'].isin(selectbox_erros_digisac)]

    # Status Corban
    if len(selectbox_status_corban) != 0:
        dados_filtrados = dados_filtrados[dados_filtrados['Status Corban'].isin(selectbox_status_corban)]

    ##### FILTRO DE INTERVALO DE DATA CONSULTA #####
    menor_data_consulta, maior_data_consulta = get_datas_consulta(dados_filtrados)
    if "filtro_periodo_consulta" not in st.session_state:
        st.session_state.filtro_periodo_consulta = (menor_data_consulta, date.today())

    if not pd.isnull(menor_data_consulta):
        intervalo_consulta = st.date_input(
            "Selecione a data da Consulta:",
            value=(menor_data_consulta, maior_data_consulta),
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
                dados_filtrados['Data Consulta'] = pd.to_datetime(dados_filtrados['Data Consulta'], errors='coerce')
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['Data Consulta'] >= pd.Timestamp(inicio_consulta)) &
                    (dados_filtrados['Data Consulta'] <= pd.Timestamp(fim_consulta))
                ]
        except:
            dados_filtrados = dados_filtrados[
                    dados_filtrados['Data Consulta'].isna()
                ]

    # ##### FILTRO DE INTERVALO DE DATA DIGISAC #####
    # menor_data_corban, maior_data_corban = get_datas_corban(dados_filtrados)
    # if "filtro_periodo_corban" not in st.session_state:
    #     st.session_state.filtro_periodo_corban = (menor_data_corban, maior_data_corban)

    # if not pd.isnull(menor_data_corban):
    #     intervalo_corban = st.date_input(
    #         "Selecione a data do Corban:",
    #         value=(menor_data_corban, maior_data_corban),
    #         key="filtro_periodo_corban"
    #     )
        
    #     # Se o usuário selecionou apenas uma data, define fim como hoje
    #     if len(intervalo_corban) == 2:
    #         inicio_corban, fim_corban = intervalo_corban
    #     elif len(intervalo_corban) == 1:
    #         # Usuário selecionou apenas uma data
    #         inicio_corban = intervalo_corban[0]
    #         fim_corban = date.today()
        
    #     try:
    #         if (inicio_corban, fim_corban) != (menor_data_corban, maior_data_corban):
    #             dados_filtrados['Data Corban'] = pd.to_datetime(dados_filtrados['Data Corban'], errors='coerce')
    #             dados_filtrados = dados_filtrados[
    #                 (dados_filtrados['Data Corban'] >= pd.Timestamp(inicio_corban)) &
    #                 (dados_filtrados['Data Corban'] <= pd.Timestamp(fim_corban))
    #             ]
    #     except:
    #         dados_filtrados = dados_filtrados[
    #                 (dados_filtrados['Data Corban'].isna()) |
    #                 (dados_filtrados['Data Corban'].isnull())
    #             ]
            
    ##### FILTRO DE INTERVALO DE DATA CORBAN #####
    menor_data_corban, maior_data_corban = get_datas_corban(dados_filtrados)
    if "filtro_periodo_corban" not in st.session_state:
        st.session_state.filtro_periodo_corban = (menor_data_corban, maior_data_corban)

    if not pd.isnull(menor_data_corban):
        intervalo_corban = st.date_input(
            "Selecione a data do Corban:",
            value=(menor_data_corban, maior_data_corban),
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
                dados_filtrados['Data Corban'] = pd.to_datetime(dados_filtrados['Data Corban'], errors='coerce')
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['Data Corban'] >= pd.Timestamp(inicio_corban)) &
                    (dados_filtrados['Data Corban'] <= pd.Timestamp(fim_corban))
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

##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1a, col_2a = st.columns((2, 8))

    with col_1a:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2a:
        st.title(":blue[Análise dos Clientes]")


st.dataframe(dados_filtrados, width='stretch', height=500, hide_index=True)

with st.container():
    col_1, col_2, col_3 = st.columns((1,1,8))
    with col_1:
        ##### BOTÃO EXPORTAR TABELA #####
        dados_xlsx = dados_filtrados[['Nome','Telefone']]
        dados_xlsx = dados_xlsx[~dados_xlsx['Telefone'].isnull()].drop_duplicates(subset='Telefone')
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            dados_xlsx.to_excel(writer, index=False, sheet_name='Planilha1')
        buffer.seek(0)  # volta o ponteiro para o início

        # --- Botão para download ---
        st.download_button(
            label="⬇️ Baixar planilha Excel",
            data=buffer,
            file_name="dados.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    with col_2:
        texto_tabela = 'teste'
        csv = dados_filtrados.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Baixar visualização",
            data=csv,
            file_name=f"clientes_{texto_tabela}.csv",
            mime="text/csv"
        )