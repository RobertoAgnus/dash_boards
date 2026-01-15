import re
import unicodedata
import numpy as np
import pandas as pd
import altair as alt
import streamlit as st
from datetime import date
from querys.connect import Conexao
from querys.querys_sql import QuerysSQL
from regras.formatadores import formatar_cpf, formatar_telefone
import warnings
warnings.filterwarnings("ignore")


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
    if valor is None or pd.isna(valor):
        return "0,00"

    return (
        f"{float(valor):,.2f}"
        .replace('.', '|')
        .replace(',', '.')
        .replace('|', ',')
    )


##### FUNÇÃO PARA MAPEAR MENSAGENS #####
def mapeia_mensagens(mensagem):
    if '[' in str(mensagem):
        resultado = re.search(r'\[[^\]]+\]', mensagem)

        return resultado.group() if resultado else None
    elif '(s' in str(mensagem):
        resultado = re.search(r'\([^\)]+\)', mensagem)

        return resultado.group() if resultado else None
    elif ('Falar com atendente' in str(mensagem)) or ('Falar com suporte' in str(mensagem)) or ('Ver atualização' in str(mensagem)) or ('Receber proposta' in str(mensagem)):
        return 'Disparos'
    elif ('Olá! Gostaria de fazer' in str(mensagem)) or ('Olá, quero antecipar' in str(mensagem)):
        return '(site)'
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
            <p style="color: white; font-weight: bold; font-size: clamp(0.5rem, 1.2vw, 0.9rem)">{label}</p>
            <h3 style="color: white; font-size: clamp(0.5rem, 4vw, 1.5rem)">{value}</h3>
        </div>
        """,
        unsafe_allow_html=True
    )


##### FUNÇÃO PARA OBTER AS DATAS #####
def get_datas(df, coluna):
    # Remove linhas com Data Mensagem vazia
    df = df.dropna(subset=[coluna])

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = df[coluna].min()
    maior_data = date.today()
    
    return menor_data, maior_data


##### FUNÇÃO PARA OBTER O COMPRIMENTO DO NOME #####
def tamanho_nome(nome):
    if isinstance(nome, str):
        return len(nome)
    return 0

##### FUNÇÃO PARA LIMPAR O NOME #####
def limpar_nome(nome):
    if pd.isna(nome):
        return nome

    # Remove acentos
    nome = unicodedata.normalize('NFKD', nome)
    nome = nome.encode('ASCII', 'ignore').decode('ASCII')

    # Remove caracteres especiais (mantém letras e espaço)
    nome = re.sub(r'[^A-Za-z\s]', '', nome)

    # Remove espaços duplicados
    nome = re.sub(r'\s+', ' ', nome).strip()

    return nome


##### CARREGAR OS DADOS (1x) #####
conectar = Conexao()

conectar.conectar_postgres_aws()
conectar.conectar_postgres()

conn_postgres_aws = conectar.obter_conexao_postgres_aws()
conn_postgres     = conectar.obter_conexao_postgres()

consulta = QuerysSQL()

digisac, corban, crm = consulta.get_campanhas()
campanhas            = consulta.get_campanhas_meta()

# df_digisac      = pd.read_sql_query(digisac, conn_postgres)
df_corban       = pd.read_sql_query(corban, conn_postgres)
df_crm          = pd.read_sql_query(crm, conn_postgres_aws)
custo_campanhas = pd.read_sql_query(campanhas, conn_postgres)

# Remove telefone inválido
# df_corban = df_corban[df_corban['numero_corban'] != '99999999999']

# Realiza merge entre as bases
df_crm_corban = pd.merge(df_crm, df_corban, left_on=['cpf'], right_on=['cpf_corban'], how='outer')    #left_on=['cpf'], right_on=['cpf_corban'], how='outer')
# df_crm_corban

df_crm_corban['nome_x'            ] = np.where(df_crm_corban['nome_y'          ].isna(), df_crm_corban['nome_x'            ], df_crm_corban['nome_y'            ])
df_crm_corban['cpf'               ] = np.where(df_crm_corban['cpf_corban'      ].isna(), df_crm_corban['cpf'               ], df_crm_corban['cpf_corban'        ])
df_crm_corban['nome_banco_x'      ] = np.where(df_crm_corban['nome_banco_y'    ].isna(), df_crm_corban['nome_banco_x'      ], df_crm_corban['nome_banco_y'      ])
df_crm_corban['dataPagamento'     ] = np.where(df_crm_corban['liberacao'       ].isna(), df_crm_corban['dataPagamento'     ], df_crm_corban['liberacao'         ])
df_crm_corban['valorBruto'        ] = np.where(df_crm_corban['valor_financiado'].isna(), df_crm_corban['valorBruto'        ], df_crm_corban['valor_financiado'  ])
df_crm_corban['valorLiberado'     ] = np.where(df_crm_corban['valor_liberado'  ].isna(), df_crm_corban['valorLiberado'     ], df_crm_corban['valor_liberado'    ])
df_crm_corban['valor_parcela_x'   ] = np.where(df_crm_corban['valor_parcela_y' ].isna(), df_crm_corban['valor_parcela_x'   ], df_crm_corban['valor_parcela_y'   ])
df_crm_corban['prazo_x'           ] = np.where(df_crm_corban['prazo_y'         ].isna(), df_crm_corban['prazo_x'           ], df_crm_corban['prazo_y'           ])
df_crm_corban['valorTotalComissao'] = np.where(df_crm_corban['valor_comissao'  ].isna(), df_crm_corban['valorTotalComissao'], df_crm_corban['valor_comissao'    ])
df_crm_corban['createdAt'         ] = np.where(df_crm_corban['liberacao'       ].isna(), df_crm_corban['createdAt'         ], df_crm_corban['liberacao'         ])
df_crm_corban['numero'            ] = np.where(df_crm_corban['numero_corban'   ].isna(), df_crm_corban['numero'            ], df_crm_corban['numero_corban'     ])
df_crm_corban['mensagemInicial'   ] = np.where(df_crm_corban['mensagemInicial' ].isna(), 'Não Identificado'                 , df_crm_corban['mensagemInicial'   ])

df_crm_corban['createdAt'] = (
    pd.to_datetime(df_crm_corban['createdAt'], utc=True)
    .dt.tz_localize(None)
)

df_crm_corban = (
    df_crm_corban
    .sort_values('createdAt')
    .groupby('cpf', as_index=False)
    .tail(1)
)

df_crm_corban['nome_banco_x'      ] = np.where(df_crm_corban['createdAt'] > df_crm_corban['liberacao'], None, df_crm_corban['nome_banco_x'      ])
df_crm_corban['dataPagamento'     ] = np.where(df_crm_corban['createdAt'] > df_crm_corban['liberacao'], None, df_crm_corban['dataPagamento'     ])
df_crm_corban['valorBruto'        ] = np.where(df_crm_corban['createdAt'] > df_crm_corban['liberacao'], None, df_crm_corban['valorBruto'        ])
df_crm_corban['valorLiberado'     ] = np.where(df_crm_corban['createdAt'] > df_crm_corban['liberacao'], None, df_crm_corban['valorLiberado'     ])
df_crm_corban['valor_parcela_x'   ] = np.where(df_crm_corban['createdAt'] > df_crm_corban['liberacao'], None, df_crm_corban['valor_parcela_x'   ])
df_crm_corban['prazo_x'           ] = np.where(df_crm_corban['createdAt'] > df_crm_corban['liberacao'], None, df_crm_corban['prazo_x'           ])
df_crm_corban['valorTotalComissao'] = np.where(df_crm_corban['createdAt'] > df_crm_corban['liberacao'], None, df_crm_corban['valorTotalComissao'])

df_crm_corban = df_crm_corban[['numero', 'cpf', 'nome_x', 'createdAt', 'mensagemInicial', 'nome_banco_x', 'dataPagamento', 'valorBruto', 'valorLiberado', 'valor_parcela_x', 'prazo_x', 'valorTotalComissao']]

df_crm_corban = df_crm_corban.rename(
    columns={
        'nome_x': 'nome', 
        'nome_banco_x': 'nome_banco', 
        'valor_parcela_x': 'valor_parcela', 
        'prazo_x': 'prazo'
    }
)

df_crm_corban = df_crm_corban.drop_duplicates(subset=['numero', 'createdAt', 'nome_banco'])

df_crm = df_crm_corban.copy()

df_crm['valorTotalComissao'] = np.where(df_crm['dataPagamento'].isna(), 0, df_crm['valorTotalComissao'])

df_crm = formatar_cpf(df_crm, 'cpf')
df_crm = formatar_telefone(df_crm, 'numero')

df_crm = df_crm.rename(columns={
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

# dados_filtrados = dados_filtrados.drop_duplicates()

dados_filtrados['mensagens'] = dados_filtrados['Mensagem Inicial'].apply(mapeia_mensagens)

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
        

    ##### FILTRO DE INTERVALO DE DATA MENSAGEM #####
    dados_filtrados['Data da Mensagem'] = (
        pd.to_datetime(dados_filtrados['Data da Mensagem'], errors='coerce', utc=True)
        .dt.tz_localize(None)
        .dt.date
    )

    menor_data_mensagem, maior_data_mensagem = get_datas(dados_filtrados, 'Data da Mensagem')
    if "filtro_periodo_mensagem" not in st.session_state:
        st.session_state.filtro_periodo_mensagem = (menor_data_mensagem, date.today())

    if not pd.isnull(menor_data_mensagem):
        intervalo_mensagem = st.date_input(
            "Selecione a data da Mensagem:",
            value=(),
            key="filtro_periodo_mensagem"
        )
        
        # Se o usuário selecionou apenas uma data, define fim como hoje
        if len(intervalo_mensagem) == 2:
            inicio_mensagem, fim_mensagem = intervalo_mensagem
            
        elif len(intervalo_mensagem) == 1:
            # Usuário selecionou apenas uma data
            inicio_mensagem = intervalo_mensagem[0]
            fim_mensagem = date.today()
            
        # Se o usuário não alterou o intervalo, mantém todas as linhas (inclusive NaT)
        try:
            
            if (inicio_mensagem, fim_mensagem) != (menor_data_mensagem, maior_data_mensagem):
                # Filtra as linhas de consulta dentro do intervalo
                dados_filtrados = dados_filtrados[
                    (dados_filtrados['Data da Mensagem'] >= inicio_mensagem) &
                    (dados_filtrados['Data da Mensagem'] <= fim_mensagem)
                ]
                
        except:
            
            dados_filtrados = dados_filtrados[
                    dados_filtrados['Data da Mensagem'].isna()
                ]

    ##### FILTRO DE INTERVALO DE DATA LIBERAÇÃO #####
    dados_filtrados['Data da Liberação'] = (
        pd.to_datetime(dados_filtrados['Data da Liberação'], errors='coerce', utc=True)
        .dt.tz_localize(None)
        .dt.date
    )
    
    menor_data_liberacao, maior_data_liberacao = get_datas(dados_filtrados, 'Data da Liberação')
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

##### FORMATAÇÕES FINAIS DOS DADOS #####
dados_filtrados['Financiado'] = dados_filtrados['Financiado'].astype(float).apply(formata_float)
dados_filtrados['Liberado'  ] = dados_filtrados['Liberado'  ].apply(formata_float)
dados_filtrados['Parcela'   ] = dados_filtrados['Parcela'   ].apply(formata_float)
dados_filtrados['Comissão'  ] = dados_filtrados['Comissão'  ].apply(formata_float)

dados_filtrados['Data da Mensagem' ] = pd.to_datetime(dados_filtrados['Data da Mensagem' ]).dt.strftime('%d/%m/%Y')
# dados_filtrados['Data da Liberação'] = pd.to_datetime(dados_filtrados['Data da Liberação']).dt.strftime('%d/%m/%Y')

df_controle = dados_filtrados.copy()

df_controle['Liberado'] = np.where(df_controle['Liberado'].empty, '0,00', df_controle['Liberado'])
df_controle['Comissão'] = np.where(df_controle['Comissão'].empty, '0,00', df_controle['Comissão'])

df_controle['Liberado'] = df_controle['Liberado'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
df_controle['Comissão'] = df_controle['Comissão'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)

controle = df_controle.groupby(['mensagens', 'Data da Mensagem']).agg({'numero': 'count', 'Data da Liberação': 'count', 'Liberado': 'sum', 'Comissão': 'sum'}).reset_index()

controle = controle.rename(columns={'mensagens': 'Campanhas', 'numero': 'Leads', 'Data da Liberação': 'Pagos', 'Liberado': 'Valor de Produção', 'Comissão': 'Comissão Recebida'})

def mapeia_campanha(valor):
    return valor.replace('[CAMPEÕES ', '[').replace('TRABALHA +1 ANO', 'CR+1').replace('CAIXA DE PERGUNTAS', 'CRCP').replace('CR ', 'CR')


custo_campanhas = custo_campanhas.rename(columns={'data': 'Data da Mensagem', 'nome': 'Campanhas', 'valor': 'Investimento'})
custo_campanhas['Campanhas'] = custo_campanhas['Campanhas'].apply(mapeia_campanha)

custo_campanhas['Data da Mensagem']  = pd.to_datetime(custo_campanhas['Data da Mensagem']).dt.strftime('%d/%m/%Y')

controle = pd.merge(controle, custo_campanhas, on=['Data da Mensagem', 'Campanhas'], how='left')

controle = controle.groupby(['Campanhas']).sum().reset_index()

controle = controle[['Campanhas', 'Leads', 'Pagos', 'Valor de Produção', 'Comissão Recebida', 'Investimento']]

media = (
    controle
    .groupby('Campanhas', as_index=False)
    .agg(
        valor_total_produzido=('Valor de Produção', 'sum'),
        valor_total_investido=('Investimento', 'sum'),
        valor_total_comissao=('Comissão Recebida', 'sum'),
        quantidade_total=('Pagos', 'sum')
    )
)

controle['Ticket Médio'] = (
    media['valor_total_produzido'] / media['quantidade_total']
)

controle['CAC'] = (
    controle['Investimento']
    .div(controle['Pagos'])
    .replace([np.inf, -np.inf], 0)
    .fillna(0)
)

mask_zero = (controle['Investimento'] == 0) & (controle['Comissão Recebida'] > 0)
mask_normal = controle['Investimento'] > 0

controle['ROI'] = np.where(
    controle['Investimento'] == 0,
    1,
    (media['valor_total_comissao'] - media['valor_total_investido']) /
    media['valor_total_investido']
)

controle['ROI'] = np.where(controle['ROI'] >= 0, ['🟢 +' + formata_float(x) for x in controle['ROI']], ['🔴 ' + formata_float(x) for x in controle['ROI']])

##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1a, col_2a = st.columns((2, 8))

    with col_1a:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2a:
        st.title(":blue[Análise das Campanhas]")

soma_leads = (
    controle['Leads']
    .astype(int)
    .sum()
)

soma_investimento = (
    controle['Investimento']
    .astype(float)
    .sum()
)

soma_pagos = (
    controle['Pagos']
    .astype(int)
    .sum()
)

soma_liberado = (
    controle['Valor de Produção']
    .astype(float)
    .sum()
)

soma_comissao = (
    controle['Comissão Recebida']
    .astype(float)
    .sum()
)

soma_ticket = (
    soma_liberado / soma_pagos
)

soma_ticket = 0.0 if pd.isna(soma_ticket) else soma_ticket

if pd.isna(soma_pagos) or soma_pagos == 0:
    soma_cac = 0.0
else:
    soma_cac = soma_investimento / soma_pagos

if pd.isna(soma_investimento) or soma_investimento == 0:
    soma_roi = 0.0
else:
    soma_roi = (soma_comissao - soma_investimento) / soma_investimento

soma_roi = np.where(soma_roi >= 0, f"🟢 +{soma_roi:,.2f}".replace('.','|').replace(',','.').replace('|',','), f"🔴 {soma_roi:,.2f}".replace('.','|').replace(',','.').replace('|',','))

controle['Valor de Produção'] = controle['Valor de Produção'].apply(formata_float)
controle['Comissão Recebida'] = controle['Comissão Recebida'].apply(formata_float)
controle['Ticket Médio'] = controle['Ticket Médio'].apply(formata_float)
controle['Investimento'] = controle['Investimento'].apply(formata_float)
controle['CAC'] = controle['CAC'].apply(formata_float)


with st.container():
    st.subheader(":blue[Controle de Tráfego]")
    col_1, col_2, col_3, col_4, col_5, col_6, col_7, col_8 = st.columns((2, 2, 2, 2, 2, 2, 2, 2))
    
    ##### ÁREA DOS CARDS #####
    with col_1:
        ##### CARD TOTAL LEADS #####
        metric_card("Total Leads", f"{soma_leads}")

    with col_2:
        ##### CARD TOTAL INVESTIMENTO #####
        metric_card("Total Investimento", f"R$ {soma_investimento:,.2f}".replace('.','|').replace(',','.').replace('|',','))

    with col_3:
        ##### CARD TOTAL PAGOS #####
        metric_card("Total Pagos", f"{soma_pagos}")

    with col_4:
        ##### CARD TOTAL LIBERADO #####
        metric_card("Total Liberado", f"R$ {soma_liberado:,.2f}".replace('.','|').replace(',','.').replace('|',','))

    with col_5:
        ##### CARD TOTAL COMISSÃO #####
        metric_card("Total Comissão", f"R$ {soma_comissao:,.2f}".replace('.','|').replace(',','.').replace('|',','))

    with col_6:
        ##### CARD TOTAL TICKET MÉDIO #####
        metric_card("Total Ticket Médio", f"R$ {soma_ticket:,.2f}".replace('.','|').replace(',','.').replace('|',','))

    with col_7:
        ##### CARD TOTAL CAC #####
        metric_card("Total CAC", f"R$ {soma_cac:,.2f}".replace('.','|').replace(',','.').replace('|',','))

    with col_8:
        ##### CARD TOTAL ROI #####
        metric_card("Total ROI", f"{soma_roi}")


##### ÁREA DA TABELA #####
with st.container():
    st.dataframe(controle, width='stretch', height=500, hide_index=True)

dados_filtrados = dados_filtrados[['numero','CPF','Nome','Data da Mensagem','Mensagem Inicial','Banco','Data da Liberação','Financiado','Liberado','Parcela','Prazo','Comissão']]
dados_filtrados['Data da Liberação'] = (
    pd.to_datetime(
        dados_filtrados['Data da Liberação'],
        errors='coerce'
    )
    .dt.strftime('%d/%m/%Y')
)

with st.container():
    st.subheader(":blue[Controle de Interações]")
    st.dataframe(dados_filtrados, width='stretch', height=500, hide_index=True)

