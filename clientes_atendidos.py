import streamlit as st
import altair as alt
import pandas as pd
import numpy as np
import duckdb as dk
from datetime import date, datetime
from io import BytesIO
import io
import zipfile

from querys.connect import Conexao
from fontes import telefones_corban, disparos, base_consolidada, crm, digisac, corban
from regras.tratar_df_final import trata_df_final
from regras.merges import merge_crm_telefones_corban, merge_crm_disparos, merge_crm_base_consolidada, merge_digisac_telefones_corban, merge_digisac_disparos, merge_digisac_base_consolidada, merge_crm_digisac, merge_df1_corban, merge_df2_telefones_corban, merge_df3_disparos, merge_df4_base_consolidada

pd.set_option('display.max_columns', None)


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Clientes Atendidos",
    page_icon="image/logo_agnus.ico",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

##### CACHE DE CONSULTAS #####
@st.cache_data(show_spinner=False)
def carregar_dados():
    conectar = Conexao()

    conectar.conectar_mysql()
    conectar.conectar_postgres()

    conn_mysql = conectar.obter_conexao_mysql()
    conn_postgres = conectar.obter_conexao_postgres()
    
    ########## TELEFONES CORBAN ##########
    df_telefones_corban = telefones_corban.get_telefones_corban(conn_postgres)
    ######################################
    
    ########## DISPAROS ##########
    df_disparos = disparos.get_disparos(conn_postgres)
    ##############################

    ########## BASES CONSOLIDADAS ##########
    df_base_consolidada = base_consolidada.get_base_consolidada(conn_postgres)
    ########################################
    
    ########## CRM ##########
    df_crm = crm.get_crm(conn_mysql)
    #########################
    
    ########## DIGISAC ##########
    df_digisac = digisac.get_digisac(conn_postgres)
    #############################
    
    ########## CORBAN ##########
    df_corban = corban.get_corban(conn_postgres)
    ############################

    # ########## DF0 = CRM <- TELEFONES CORBAN ##########
    # df_crm = merge_crm_telefones_corban(df_crm, df_telefones_corban)
    # ##########################################
    
    # ########## DF0 = CRM <- DISPAROS ##########
    # df_crm = merge_crm_disparos(df_crm, df_disparos)
    # ##########################################

    # ########## DF0 = CRM <- BASES CONSOLIDADAS ##########
    # df_crm = merge_crm_base_consolidada(df_crm, df_base_consolidada)
    # ##########################################

    # ########## DF0 = DIGISAC <- TELEFONES CORBAN ##########
    # df_digisac = merge_digisac_telefones_corban(df_digisac, df_telefones_corban)
    # ##########################################
    
    # ########## DF0 = DIGISAC <- DISPAROS ##########
    # df_digisac = merge_digisac_disparos(df_digisac, df_disparos)
    # ##########################################

    # ########## DF0 = DIGISAC <- BASES CONSOLIDADAS ##########
    # df_digisac = merge_digisac_base_consolidada(df_digisac, df_base_consolidada)
    # ##########################################

    ########## DF1 = CRM <- DIGISAC ##########
    df1 = merge_crm_digisac(df_crm, df_digisac)
    ##########################################
    
    ########## DF2 = DF1 <- CORBAN ##########
    df2 = merge_df1_corban(df1, df_corban)
    ########################################
    
    ########## DF3 = DF2 <- TELEFONES CORBAN ##########
    df3 = merge_df2_telefones_corban(df2, df_telefones_corban)
    ########################################
    
    ########## DF4 = DF3 <- DISPAROS ##########
    df4 = merge_df3_disparos(df3, df_disparos)
    ########################################
    
    ########## DF = DF4 <- BASES CONSOLIDADAS ##########
    df = merge_df4_base_consolidada(df4, df_base_consolidada)
    ########################################
    
    df = trata_df_final(df)

    return df.drop_duplicates(subset=['CPF', 'Telefone', 'Data Consulta', 'Data Disparo', 'Data Corban'])
    

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
        dados_filtrados['Retorno Consulta'] = dados_filtrados['Retorno Consulta'].astype(str).str.strip()
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
dados_filtrados['Data Disparo' ] = pd.to_datetime(dados_filtrados['Data Disparo' ], errors='coerce')
dados_filtrados['Data Corban'  ] = pd.to_datetime(dados_filtrados['Data Corban'  ], errors='coerce')

dados_filtrados['Data Consulta'] = dados_filtrados['Data Consulta'].dt.strftime('%d/%m/%Y')
dados_filtrados['Data Disparo' ] = dados_filtrados['Data Disparo' ].dt.strftime('%d/%m/%Y')
dados_filtrados['Data Corban'  ] = dados_filtrados['Data Corban'  ].dt.strftime('%d/%m/%Y')

# dados_filtrados['Telefone'] = dados_filtrados['Telefone'].astype(str).apply(
#         lambda x: x[:2] + '9' + x[2:] if len(x) <= 10 and x.isdigit() else x
#     )

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

    # st.write(f'Sem Nome: {len(dados_filtrados[dados_filtrados["Nome"].isna()])}')
        