from datetime import date, datetime
import streamlit as st
import pandas as pd
import altair as alt

pd.set_option('display.max_columns', None)

from querys.querys_sql import QuerysSQL
from querys.connect import Conexao


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Comissões Corban",
    page_icon="image/logo_agnus.ico",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

##### CRIAR INSTÂNCIA ÚNICA #####
consulta = QuerysSQL()

##### CACHE DE CONSULTAS #####
@st.cache_data(show_spinner=False)
def carregar_dados():
    conectar = Conexao()
    conectar.conectar_postgres()
    conn = conectar.obter_conexao_postgres()

    api = consulta.api()
    comissionamento = consulta.comissionamento()
    comissao = consulta.comissoes()
    data = consulta.datas()
    proposta = consulta.proposta()
    propostas = consulta.propostas()

    df_api = pd.read_sql_query(api, conn)
    df_comissionamento = pd.read_sql_query(comissionamento, conn)
    df_comissao = pd.read_sql_query(comissao, conn)
    df_data = pd.read_sql_query(data, conn)
    df_proposta = pd.read_sql_query(proposta, conn)
    df_propostas = pd.read_sql_query(propostas, conn)

    dados = {
                'api': df_api, 
                'comissionamento': df_comissionamento,
                'comissao': df_comissao,
                'data': df_data,
                'proposta': df_proposta,
                'propostas': df_propostas
            }

    conectar.desconectar_postgres()

    return dados

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
    
def get_datas_propostas(dados):
    df_datas = dados['propostas']
    
    # Converting the 'data' column to datetime format (caso não esteja)
    df_datas['data'] = pd.to_datetime(df_datas['data_status'], dayfirst=True)

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = df_datas['data'].min()
    maior_data = date.today()

    return menor_data, maior_data

def get_origem_propostas(dados):
    origem = dados['propostas']

    mapeamento = {
                    "BioWpp": ["BioWpp"],
                    "Chat Bot": ["Chat Bot"],
                    "Disparo": ["Disparo"],
                    "Indicação": ["INDICAÇÃO"],
                    "Instagram": ["Instagram"],
                    "Não Identificado": ["NÃO IDENTIFICADO"],
                    "Planilha": ["Planilha"],
                    "Site": ["Site"],
                    "TINTIN": ["TINTIN"],
                    "Facebook": ["FA","Facebook","Facebook (M1)","FB","FC","FD","TA","TB","TC","TD","TE","Trafego","CMP [M1]","CMP [T01]","CMP [TP1]","CMP [TP2]","CP 01","CP 02","CLT trafego"],
                    "TikTok": ["TKT 4","TKT 5","TN1","TN2","TQ","Tráfego - Tiktok1","Tráfego - Tiktok2","Tráfego - Tiktok3"],
                    "SMS": ["SMS 01","SMS 02","SMS 03","SMS 04"],
                    "carteira": ["Cliente de Carteira","CLT carteira"]
                }
    
    mapa_invertido = {
        variante: chave
        for chave, variantes in mapeamento.items()
        for variante in variantes
    }

    origem["origem"] = origem["origem"].map(mapa_invertido).fillna(origem["origem"])

    origem = origem.drop_duplicates(subset='origem').sort_values('origem')

    return origem['origem']

def trata_datas(intervalo):
    # Trata o intervalo de data para busca no banco de dados
    if len(intervalo) == 2:
        data_inicio, data_fim = intervalo
    
    elif len(intervalo) == 1:
        data_inicio = intervalo[0]
        data_fim = datetime.strptime(date.today().strftime("%Y-%m-%d"), "%Y-%m-%d")
    
    else:
        data_inicio, data_fim = get_datas_propostas(dados)

    data_inicio = pd.to_datetime(data_inicio)
    data_fim = pd.to_datetime(data_fim)
    
    return data_inicio, data_fim

def get_qtd_comissoes_total(dados, origem, intervalo):
    comissoes = dados['comissao']
    propostas = dados['propostas']

    df = pd.merge(comissoes, propostas, on='proposta_id', how='left')
    
    data_inicio, data_fim = trata_datas(intervalo)

    df['data_x'] = pd.to_datetime(df['data_x'], errors='coerce')
    
    if (len(origem) == 1 and origem[0] == 'Selecionar') \
        or len(origem) == 0:
        condicao = (df['data_x'] >= data_inicio) & (df['data_x'] <= data_fim)
    elif (len(origem) > 1 and origem[0] == 'Selecionar') \
        or (len(origem) >= 1 and 'Selecionar' not in origem):
        condicao = (df['origem'].isin(origem)) & ((df['data_x'] >= data_inicio) & (df['data_x'] <= data_fim))

    df = df[condicao]
    
    return df['valor'].sum()

def get_qtd_comissoes_pagas(dados, origem, intervalo):
    api       = dados['api']
    comissoes = dados['comissao']
    datas     = dados['data']
    proposta  = dados['proposta']
    propostas = dados['propostas']

    df1 = pd.merge(api, comissoes, on='proposta_id', how='left')
    df2 = pd.merge(df1, datas    , on='proposta_id', how='left')
    df3 = pd.merge(df2, proposta , on='proposta_id', how='left')
    df  = pd.merge(df3, propostas, on='proposta_id', how='left')
    
    data_inicio, data_fim = trata_datas(intervalo)
    
    df['data_status_api'] = pd.to_datetime(df['data_status_api'], errors='coerce')

    if (len(origem) == 1 and origem[0] == 'Selecionar') \
        or len(origem) == 0:
        condicao = (df['data_status_api'] >= data_inicio) & (df['data_status_api'] <= data_fim)
    elif (len(origem) > 1 and origem[0] == 'Selecionar') \
        or (len(origem) >= 1 and 'Selecionar' not in origem):
        condicao = (df['origem'].isin(origem)) & ((df['data_status_api'] >= data_inicio) & (df['data_status_api'] <= data_fim))
    
    df = df[(df['valor'] != 0)
            & (df['cancelado'].isnull())
            & (~df['pagamento'].isnull()) 
            & condicao 
            & (df['status_api'] == 'APROVADA')
            & (df['valor_total_comissionado'] != 0)
            & (df['status_nome'] != 'Cancelado')
            & (~df['status_nome'].isnull())]

    return df['valor'].sum(), len(df.drop_duplicates(subset='proposta_id'))

def get_qtd_comissao_aguardando(dados, origem, intervalo):
    api             = dados['api']
    comissionamento = dados['comissionamento']
    comissoes       = dados['comissao']
    datas           = dados['data']
    proposta        = dados['proposta']
    propostas       = dados['propostas']

    df1 = pd.merge(api, comissoes      , on='proposta_id', how='left')
    df2 = pd.merge(df1, comissionamento, on='proposta_id', how='left')
    df3 = pd.merge(df2, datas          , on='proposta_id', how='left')
    df4 = pd.merge(df3, proposta       , on='proposta_id', how='left')
    df  = pd.merge(df4, propostas      , on='proposta_id', how='left')
    
    data_inicio, data_fim = trata_datas(intervalo)
    
    df['data_status_api'] = pd.to_datetime(df['data_status_api'], errors='coerce')
    
    if (len(origem) == 1 and origem[0] == 'Selecionar') or len(origem) == 0:
        condicao = (df['data_status_api'] >= data_inicio) & (df['data_status_api'] <= data_fim)
    elif len(origem) > 1 and origem[0] == 'Selecionar':
        condicao = (df['origem'].isin(origem)) & ((df['data_status_api'] >= data_inicio) & (df['data_status_api'] <= data_fim))
    else:
        condicao = (df['origem'].isin(origem)) & ((df['data_status_api'] >= data_inicio) & (df['data_status_api'] <= data_fim))

    df = df[(df['valor'].isnull())
            & (df['cancelado'].isnull())
            & condicao 
            & (df['status_api'] == 'APROVADA')
            & (df['valor_total_comissionado'] == 0)
            & (df['status_nome'] != 'Cancelado')]
    
    return df['recebe_valor_base'].sum()

def get_detalhamento_operacoes(dados, origem, intervalo):
    comissoes = dados['comissao']
    propostas = dados['propostas']

    df = pd.merge(comissoes, propostas, on='proposta_id', how='left')
    
    data_inicio, data_fim = trata_datas(intervalo)

    df['data_x'] = pd.to_datetime(df['data_x'], errors='coerce')

    if (len(origem) == 1 and origem[0] == 'Selecionar') \
        or len(origem) == 0:
        condicao = (df['data_x'] >= data_inicio) & (df['data_x'] <= data_fim)
    elif (len(origem) > 1 and origem[0] == 'Selecionar') \
        or (len(origem) >= 1 and 'Selecionar' not in origem):
        condicao = (df['origem'].isin(origem)) & ((df['data_x'] >= data_inicio) & (df['data_x'] <= data_fim))

    df = df[condicao]

    df = df[['data_x', 'proposta_id', 'origem', 'valor']].sort_values('data_x')
    df = df.rename(columns={'data_x': 'Data da Comissão', 'origem': 'Origem', 'valor': 'Valor', 'proposta_id': 'Proposta'})

    df_agrupado = df.groupby(['Data da Comissão', 'Proposta', 'Origem'], as_index=False).agg({'Valor': 'sum'})

    return df_agrupado


dados = carregar_dados()

##### ORIGEM DAS PROPOSTAS CONTRATADAS #####
lista_origem = get_origem_propostas(dados)

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE ORIGEM #####
    if "filtro_origem" not in st.session_state:
        st.session_state.filtro_origem = ["Selecionar"]

    selectbox_origem = st.multiselect(
        'Selecione a origem',
        ['Selecionar'] + lista_origem.tolist(),
        key="filtro_origem"
    )
    
    ##### FILTRO DE DATA #####
    menor_data, maior_data = get_datas_propostas(dados)
    
    if "filtro_periodo" not in st.session_state:
        hoje = date.today()
        st.session_state.filtro_periodo = (menor_data, hoje) 

    intervalo = st.date_input(
        "Selecione um intervalo de datas:",
        value=(menor_data,maior_data),
        key="filtro_periodo"
    )

    # Botão de limpeza
    if st.button("🧹 Limpar filtros"):
        st.session_state.clear()
        st.rerun()


##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1, col_2 = st.columns((2, 8))

    with col_1:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2:
        st.title(":blue[Análise das Comissões]")

##### CORPO DO DASHBOARD #####
with st.container():
    col_1, col_2, col_3 = st.columns((3.33, 3.33, 3.33), gap="medium")

    ##### ÁREA DOS CARDS #####
    with col_1:
        ##### TOTAL DE COMISSÕES #####
        total_comissao = get_qtd_comissoes_total(dados, selectbox_origem, intervalo)

        valor_total = f"R$ {format(float(total_comissao), ',.2f').replace('.','|').replace(',','.').replace('|',',')}"
        
        metric_card("Valor Total", f"{valor_total}")

    with col_2:
        ##### TOTAL DECOMISSÕES PAGAS #####
        comissoes_pagas, qtd_operacoes = get_qtd_comissoes_pagas(dados, selectbox_origem, intervalo)

        valor_pago = f"R$ {format(float(comissoes_pagas), ',.2f').replace('.','|').replace(',','.').replace('|',',')}"

        metric_card("Valor Pago", f"{valor_pago}")

    with col_3:
        comissoes_aguardando = get_qtd_comissao_aguardando(dados, selectbox_origem, intervalo)

        valor_aguardando = f"R$ {format(float(comissoes_aguardando), ',.2f').replace('.','|').replace(',','.').replace('|',',')}"

        metric_card("Valor Aguardando", f"{valor_aguardando}")

with st.container():
    col_1, col_2, col_3 = st.columns((3.33, 3.33, 3.33), gap="medium")

    with col_1:
        if total_comissao == 0:
            taxa_pagamento = 0.0
        else:
            taxa_pagamento = (float(comissoes_pagas) / float(total_comissao)) * 100

        metric_card("Taxa de Pagamento", f"{format(float(taxa_pagamento), ',.2f').replace('.',',')} %")

    with col_2:
        if total_comissao == 0:
            taxa_pendencia = 0.0
        else:
            taxa_pendencia = (float(comissoes_aguardando) / float(total_comissao)) * 100

        metric_card("Taxa de Pendência", f"{format(float(taxa_pendencia), ',.2f').replace('.',',')} %")

    with col_3: 
        if qtd_operacoes == 0:
            ticket_medio = 0.0
        else:
            ticket_medio = (float(comissoes_pagas) / float(qtd_operacoes))
            
        metric_card("Ticket Médio", f"R$ {format(float(ticket_medio), ',.2f').replace('.','|').replace(',','.').replace('|',',')}")

##### ÁREA DA TABELA #####
with st.container():
    df_comissao_agrupado = get_detalhamento_operacoes(dados, selectbox_origem, intervalo)
    
    ##### TABELA DE CLIENTES #####
    st.markdown("### :blue[Detalhamento das Comissões]")

    df_comissao_agrupado['Data da Comissão'] = pd.to_datetime(df_comissao_agrupado['Data da Comissão'])
    df_comissao_agrupado['Data da Comissão'] = df_comissao_agrupado['Data da Comissão'].dt.strftime('%d/%m/%Y')
    
    st.dataframe(df_comissao_agrupado, height=500, hide_index=True)

    ##### BOTÃO EXPORTAR TABELA #####
    csv = df_comissao_agrupado.to_csv(index=False)
    st.download_button(
        label="⬇️ Baixar planilha",
        data=csv,
        file_name="dados.csv",
        mime="text/csv",
    )
