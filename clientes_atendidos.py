import streamlit as st
import altair as alt
import pandas as pd
from datetime import date
from io import BytesIO
import io
import zipfile

from regras.obter_dados import carregar_dados
from regras.tratamentos import Tratamentos


if not st.session_state.get("authenticated", False):
    st.stop()


tratamentos = Tratamentos()


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Clientes Atendidos",
    page_icon="image/logo_agnus.ico",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

if "authenticated" not in st.session_state:
    st.error("Acesso negado. Faça login.")
    st.stop()

##### CARREGAR OS DADOS (1x) #####
dados, df_crm, df_digisac, df_corban = carregar_dados()


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

    ##### FILTRO DE BANCOS #####
    banco_consulta = dados['Banco'].dropna().unique().tolist()
    banco_consulta = [str(x).strip() for x in banco_consulta if x is not None]
    banco_consulta = sorted(banco_consulta)

    if "filtro_banco" not in st.session_state:
        st.session_state.filtro_banco = []

    selectbox_banco = st.multiselect(
        'Selecione o Banco',
        banco_consulta,
        key="filtro_banco",
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

    # Banco
    if len(selectbox_banco) != 0:
        dados_filtrados['Banco'] = dados_filtrados['Banco'].astype(str).str.strip()
        filtros = [str(x).strip() for x in selectbox_banco]
        dados_filtrados = dados_filtrados[dados_filtrados['Banco'].isin(filtros)]

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
    menor_data_consulta, maior_data_consulta = tratamentos.get_datas(dados, 'Data Consulta')
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
    menor_data_disparos, maior_data_disparos = tratamentos.get_datas(dados, 'Data Disparo')
    
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
    menor_data_corban, maior_data_corban = tratamentos.get_datas(dados, 'Data Corban')
    
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
        for key in list(st.session_state.keys()):
            if key.startswith("filtro_"):
                del st.session_state[key]
        st.rerun()

dados_filtrados['Data Consulta'] = pd.to_datetime(dados_filtrados['Data Consulta'], errors='coerce')
dados_filtrados['Data Disparo' ] = pd.to_datetime(dados_filtrados['Data Disparo' ], errors='coerce')
dados_filtrados['Data Corban'  ] = pd.to_datetime(dados_filtrados['Data Corban'  ], errors='coerce')

dados_filtrados['Data Consulta'] = dados_filtrados['Data Consulta'].dt.strftime('%d/%m/%Y')
dados_filtrados['Data Disparo' ] = dados_filtrados['Data Disparo' ].dt.strftime('%d/%m/%Y')
dados_filtrados['Data Corban'  ] = dados_filtrados['Data Corban'  ].dt.strftime('%d/%m/%Y')

dados_csv = dados_filtrados[
                    (dados_filtrados['Telefone'].notnull()) & 
                    (dados_filtrados['Data Consulta'].notnull())
                ].drop_duplicates(subset='Telefone')
dados_csv['Telefone'] = '55' + dados_csv['Telefone'].astype(str)
dados_csv = dados_csv[['Nome','Telefone']]

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
        tratamentos.metric_card("Consultas Realizadas", f"{format(int(len(dados_filtrados[(dados_filtrados['Data Consulta'].notna()) & (dados_filtrados['Data Consulta'].notnull())])), ',').replace(',', '.')}")

    with col_1b:
        ##### CARD TOTAL VISUALIZAÇÕES #####
        tratamentos.metric_card("Visualizações na Tela", f"{format(int(len(dados_filtrados)), ',').replace(',', '.')}")


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
