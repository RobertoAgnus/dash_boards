import re
import unicodedata
import numpy as np
import pandas as pd
import altair as alt
import streamlit as st
from datetime import date
from querys.connect import Conexao
from querys.querys_sql import QuerysSQL
from regras.formatadores import Regras
from regras.tratamentos import Tratamentos
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

# =============== FUNÇÕES ===============
regras      = Regras()
tratamentos = Tratamentos()


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



# =======================================


##### CARREGAR OS DADOS (1x) #####
conectar = Conexao()

conectar.conectar_postgres_aws()
conectar.conectar_postgres()

conn_postgres_aws = conectar.obter_conexao_postgres_aws()
conn_postgres     = conectar.obter_conexao_postgres()

consulta = QuerysSQL()

digisac, corban, crm    = consulta.get_campanhas()
campanhas               = consulta.get_campanhas_meta()
fones_crm, fones_corban = consulta.get_telefones()
comissoes_corban        = consulta.get_comissoes_corban()
tabelas_comissoes       = consulta.get_tabelas_comissao()

# df_digisac      = pd.read_sql_query(digisac, conn_postgres)
df_corban       = pd.read_sql_query(corban, conn_postgres)
df_crm          = pd.read_sql_query(crm, conn_postgres_aws)
custo_campanhas = pd.read_sql_query(campanhas, conn_postgres)
df_fones_crm    = pd.read_sql_query(fones_crm, conn_postgres_aws)
df_comissoes    = pd.read_sql_query(comissoes_corban, conn_postgres)
df_tabelas      = pd.read_sql_query(tabelas_comissoes, conn_postgres)


# ============= TRATAMENTOS =============
df_corban = pd.merge(df_corban, df_fones_crm, on='cpf_corban', how='left')
df_corban = pd.merge(df_corban, df_comissoes, on='proposta_id', how='left')

sem_fones = df_corban[df_corban['numero_corban'].isnull()]

cpfs_sem_fone = sem_fones['cpf_corban'].unique().tolist()

df_fones_corban = pd.read_sql_query(fones_corban, conn_postgres, params=(cpfs_sem_fone,))

df_corban = pd.merge(df_corban, df_fones_corban, on='cpf_corban', how='left')

df_corban['numero_corban'] = np.where(df_corban['numero_corban_x'].isnull(), df_corban['numero_corban_y'], df_corban['numero_corban_x'])

custo_campanhas['nome'] = custo_campanhas['nome'].apply(tratamentos.mapeia_campanha)

# ================ MERGE ================
# Realiza merge entre as bases
df = pd.merge(df_crm, df_corban, left_on=['cpf'], right_on=['cpf_corban'], how='outer', indicator=True)

# # Garante linhas únicas
# df = df.drop_duplicates()

# =============== DATAS =================
# Padroniza colunas de datas
df['createdAt'] = (
    pd.to_datetime(df['createdAt'], utc=True)
    .dt.tz_localize(None)
)

df['dataPagamento'] = (
    pd.to_datetime(df['dataPagamento'], utc=True)
    .dt.tz_localize(None)
)

df['liberacao'] = (
    pd.to_datetime(df['liberacao'], utc=True)
    .dt.tz_localize(None)
)

# df = df.drop_duplicates(subset=['numero', 'cpf', 'createdAt', 'dataPagamento'])

df['nome_x'            ] = np.where(df['nome_x'            ].isna(), df['nome_y'          ], df['nome_x'            ])
df['cpf'               ] = np.where(df['cpf'               ].isna(), df['cpf_corban'      ], df['cpf'               ])
df['nome_banco_x'      ] = np.where(df['nome_banco_x'      ].isna(), df['nome_banco_y'    ], df['nome_banco_x'      ])
df['dataPagamento'     ] = np.where(df['dataPagamento'     ].isna(), df['liberacao'       ], df['dataPagamento'     ])
df['nome_banco_x'      ] = np.where(df['dataPagamento'     ].isna(), None                  , df['nome_banco_x'      ])
df['valorBruto'        ] = np.where(df['valorBruto'        ].isna(), df['valor_financiado'], df['valorBruto'        ])
df['valorLiberado'     ] = np.where(df['valorLiberado'     ].isna(), df['valor_liberado'  ], df['valorLiberado'     ])
df['valor_parcela_x'   ] = np.where(df['valor_parcela_x'   ].isna(), df['valor_parcela_y' ], df['valor_parcela_x'   ])
df['prazo_x'           ] = np.where(df['prazo_x'           ].isna(), df['prazo_y'         ], df['prazo_x'           ])
df['valorTotalComissao'] = np.where(df['valorTotalComissao'].isna(), df['valor_comissao'  ], df['valorTotalComissao'])
df['createdAt'         ] = np.where(df['createdAt'         ].isna(), df['liberacao'       ], df['createdAt'         ])
df['codigo'            ] = np.where(df['codigo'            ].isna(), df['tabela_id'       ], df['codigo'            ])
df['numero'            ] = np.where(df['numero'            ].isna(), df['numero_corban'   ], df['numero'            ])
df['mensagemInicial'   ] = np.where(df['mensagemInicial'   ].isna(), 'Não Identificado'    , df['mensagemInicial'   ])

df['cpf'] = df['cpf'].apply(regras.limpar_cpf)

df['numero_corban_x'] = np.where(df['numero'].notna(), None, df['numero_corban_x'])
df['numero_corban_y'] = np.where(df['numero'].notna(), None, df['numero_corban_y'])
df['numero_corban'] = np.where(df['numero'].notna(), None, df['numero_corban'])

df = df.drop_duplicates()

# Dividindo em duas linhas, registros diferentes em ambos
linhas_both = df[
    (df['_merge'] == 'both') &
    (df['dataPagamento'] != df['liberacao']) &
    (
        (df['dataPagamento'].notna()) & 
        (df['liberacao'].notna())
    )
]

col_left  = ['nome_x', 'createdAt', 'mensagemInicial' , 'nome_banco_x'  , 'dataPagamento'  , 'valorBruto', 'valorLiberado', 'valor_parcela_x', 'prazo_x', 'valorTotalComissao', 'codigo']
col_right = ['nome_y', 'liberacao', 'valor_financiado', 'valor_liberado', 'valor_parcela_y', 'prazo_y'   , 'nome_banco_y' , 'valor_comissao' , 'tabela_id'                              ]

left_only = linhas_both.copy()
left_only[col_right] = None
left_only['_merge'] = 'left_only'

right_only = linhas_both.copy()
right_only[col_left] = None
right_only['_merge'] = 'right_only'

df = pd.concat(
    [
        df.drop(linhas_both.index),
        left_only,
        right_only
    ],
    ignore_index=True
)

df['nome_x'            ] = np.where(df['nome_x'            ].isna(), df['nome_y'          ], df['nome_x'            ])
df['nome_banco_x'      ] = np.where(df['nome_banco_x'      ].isna(), df['nome_banco_y'    ], df['nome_banco_x'      ])
df['dataPagamento'     ] = np.where(df['dataPagamento'     ].isna(), df['liberacao'       ], df['dataPagamento'     ])
df['nome_banco_x'      ] = np.where(df['dataPagamento'     ].isna(), None                  , df['nome_banco_x'      ])
df['valorBruto'        ] = np.where(df['valorBruto'        ].isna(), df['valor_financiado'], df['valorBruto'        ])
df['valorLiberado'     ] = np.where(df['valorLiberado'     ].isna(), df['valor_liberado'  ], df['valorLiberado'     ])
df['valor_parcela_x'   ] = np.where(df['valor_parcela_x'   ].isna(), df['valor_parcela_y' ], df['valor_parcela_x'   ])
df['prazo_x'           ] = np.where(df['prazo_x'           ].isna(), df['prazo_y'         ], df['prazo_x'           ])
df['valorTotalComissao'] = np.where(df['valorTotalComissao'].isna(), df['valor_comissao'  ], df['valorTotalComissao'])
df['createdAt'         ] = np.where(df['createdAt'         ].isna(), df['liberacao'       ], df['createdAt'         ])
df['codigo'            ] = np.where(df['codigo'            ].isna(), df['tabela_id'       ], df['codigo'            ])
df['mensagemInicial'   ] = np.where(df['mensagemInicial'   ].isna(), 'Não Identificado'    , df['mensagemInicial'   ])

df = df[['numero', 'cpf', 'nome_x', 'createdAt', 'mensagemInicial', 'nome_banco_x', 'dataPagamento', 'valorBruto', 'valorLiberado', 'valor_parcela_x', 'prazo_x', 'valorTotalComissao', 'codigo', '_merge']]

mask_datas = (
    (df['dataPagamento'].notna()) &
    (df['createdAt'] > df['dataPagamento'])
)

df['nome_banco_x'      ] = np.where(mask_datas, None, df['nome_banco_x'      ])
df['valorBruto'        ] = np.where(mask_datas, None, df['valorBruto'        ])
df['valorLiberado'     ] = np.where(mask_datas, None, df['valorLiberado'     ])
df['valor_parcela_x'   ] = np.where(mask_datas, None, df['valor_parcela_x'   ])
df['prazo_x'           ] = np.where(mask_datas, None, df['prazo_x'           ])
df['valorTotalComissao'] = np.where(mask_datas, None, df['valorTotalComissao'])
df['codigo'            ] = np.where(mask_datas, None, df['codigo'            ])
df['dataPagamento'     ] = np.where(mask_datas, pd.NaT, df['dataPagamento'     ])

# =======================================

# =============== DATAS =================
# Padroniza colunas de datas
df['createdAt'] = (
    pd.to_datetime(df['createdAt'], utc=True)
    .dt.tz_localize(None)
)

df['dataPagamento'] = (
    pd.to_datetime(df['dataPagamento'], utc=True)
    .dt.tz_localize(None)
)

df = df.drop_duplicates()

# Garantindo pagamentos mais prováveis para data da mensagem
cols_msg = ['cpf', 'numero', 'dataPagamento', 'valorLiberado']
# cols_ctt = ['cpf_corban', 'numero_corban', 'liberacao']

df_valid = df[
    df['createdAt'].notna() &
    df['dataPagamento'].notna() &
    (df['createdAt'] <= df['dataPagamento'])
].copy()

df_valid['delta'] = (
    df_valid['dataPagamento'] - df_valid['createdAt']
).dt.total_seconds()

# escolhe o contrato mais próximo para cada mensagem
idx_msg = (
    df_valid
    .sort_values('delta', ascending=True)
    .groupby(cols_msg, as_index=False)
    .head(1)
    .index
)

df_match = df_valid.loc[idx_msg].drop(columns='delta')

contratos_usados = df_match['createdAt'].unique()

df_contrato_orfao = df[
    df['dataPagamento'].notna() &
    ~df['createdAt'].isin(contratos_usados)
].copy()

# zera colunas do sistema X
for col in ['nome_banco_x', 'dataPagamento', 'valorBruto', 'valorLiberado', 'valor_parcela_x', 'prazo_x', 'valorTotalComissao', 'codigo']:
    if col in df_contrato_orfao:
        df_contrato_orfao[col] = None

df_tratado = pd.concat(
    [df_match, df_contrato_orfao],
    ignore_index=True
)

mensagens_usadas = df_tratado['createdAt'].unique()

df_msg_orfao = df[
    df['createdAt'].notna() &
    ~df['createdAt'].isin(mensagens_usadas)
].copy()

df_crm_corban = pd.concat(
    [df_msg_orfao, df_tratado],
    ignore_index=True
)

df_crm_corban['codigo'] = np.where(df_crm_corban['codigo'] == 'Diamante', '677444', df_crm_corban['codigo'])
df_crm_corban['codigo'] = np.where(df_crm_corban['codigo'] == 'Gold'    , '620175', df_crm_corban['codigo'])

df_crm_corban['codigo'] = df_crm_corban['codigo'].apply(regras.trata_codigo)

df_crm_corban = pd.merge(df_crm_corban, df_tabelas, on='codigo', how='left')

mask_comissao = (
    (
        (df_crm_corban['valorTotalComissao'].isna()) |
        (df_crm_corban['valorTotalComissao'] == 0)
    ) &
    (df_crm_corban['dataPagamento'].notna()) &
    (df_crm_corban['prazo_x'] >= df_crm_corban['prazo_inicio']) &
    (df_crm_corban['prazo_x'] <= df_crm_corban['prazo_fim'])
)

df_crm_corban['valorTotalComissao'] = np.where(mask_comissao, df_crm_corban['valorLiberado'].astype(float) * (df_crm_corban['percentual'].astype(float) / 100), df_crm_corban['valorTotalComissao'])

# linhas_excluir = df_crm_corban[
#     (df_crm_corban['dataPagamento'].notna()) &
#     (df_crm_corban['valorTotalComissao'].isna())
# ]

df_crm_corban = df_crm_corban[['numero', 'cpf', 'nome_x', 'createdAt', 'mensagemInicial', 'nome_banco_x', 'dataPagamento', 'valorBruto', 'valorLiberado', 'valor_parcela_x', 'prazo_x', 'valorTotalComissao', '_merge']]

df_crm_corban = df_crm_corban.drop_duplicates()

df_crm_corban = df_crm_corban.rename(
    columns={
        'nome_x': 'nome', 
        'nome_banco_x': 'nome_banco', 
        'valor_parcela_x': 'valor_parcela', 
        'prazo_x': 'prazo'
    }
)

df_crm_corban = regras.formatar_cpf(df_crm_corban, 'cpf')
df_crm_corban = regras.formatar_telefone(df_crm_corban, 'numero')

df_crm_corban = df_crm_corban.rename(columns={
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

df_crm_corban['Data da Mensagem'] = (
    pd.to_datetime(df_crm_corban['Data da Mensagem'], errors='coerce', utc=True)
    .dt.tz_localize(None)
    .dt.date
)

df_crm_corban['Data da Liberação'] = (
    pd.to_datetime(df_crm_corban['Data da Liberação'], errors='coerce', utc=True)
    .dt.tz_localize(None)
    .dt.date
)

df_crm_corban['mensagens'] = df_crm_corban['Mensagem Inicial'].apply(tratamentos.mapeia_mensagens)

df_crm_corban['Comissão'  ] = np.where(df_crm_corban['Data da Liberação'].isna(), None                                             , df_crm_corban['Comissão'  ])
df_crm_corban['Financiado'] = np.where(df_crm_corban['Financiado'       ] == 0  , df_crm_corban['Parcela'] * df_crm_corban['Prazo'], df_crm_corban['Financiado'])

dados_filtrados = df_crm_corban.copy()

##### ÁREA DO DASHBOARD #####

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE MENSAGENS INICIAIS #####
    mensagem_inicial = df_crm_corban['mensagens'].dropna().unique().tolist()
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
        
        custo_campanhas = custo_campanhas[custo_campanhas['nome'].isin(filtros)]
        

    ##### FILTRO DE INTERVALO DE DATA MENSAGEM #####
    dados_filtrados['Data da Mensagem'] = (
        pd.to_datetime(dados_filtrados['Data da Mensagem'], errors='coerce', utc=True)
        .dt.tz_localize(None)
        .dt.date
    )

    menor_data_mensagem, maior_data_mensagem = tratamentos.get_datas(df_crm_corban, 'Data da Mensagem')
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
                custo_campanhas = custo_campanhas[
                    (custo_campanhas['data'] >= inicio_mensagem) &
                    (custo_campanhas['data'] <= fim_mensagem)
                ]
                
        except:
            
            dados_filtrados = dados_filtrados[
                dados_filtrados['Data da Mensagem'].isna()
            ]
            custo_campanhas = custo_campanhas[
                custo_campanhas['data'].isna()
            ]

    ##### FILTRO DE INTERVALO DE DATA LIBERAÇÃO #####
    dados_filtrados['Data da Liberação'] = (
        pd.to_datetime(dados_filtrados['Data da Liberação'], errors='coerce', utc=True)
        .dt.tz_localize(None)
        .dt.date
    )
    
    menor_data_liberacao, maior_data_liberacao = tratamentos.get_datas(df_crm_corban, 'Data da Liberação')
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
                custo_campanhas = custo_campanhas[
                    (custo_campanhas['data'] >= inicio_liberacao) &
                    (custo_campanhas['data'] <= fim_liberacao)
                ]
                
        except:
            
            dados_filtrados = dados_filtrados[
                dados_filtrados['Data da Liberação'].isna()
            ]
            custo_campanhas = custo_campanhas[
                custo_campanhas['data'].isna()
            ]
        
    ##### FILTRO DE EXIBIÇÃO #####
    if "selecao_exibicao" not in st.session_state:
        st.session_state.selecao_exibicao = "Planilha"

    lista_opcao = ["Planilha", "Gráfico"]

    seleciona_exibicao = st.radio(
        'Selecione Opção',
        lista_opcao,
        key='selecao_exibicao'
    )

    # Botão de limpeza
    if st.button("🧹 Limpar filtros"):
        for key in list(st.session_state.keys()):
            if key.startswith("filtro_"):
                del st.session_state[key]
        st.rerun()

##### FORMATAÇÕES FINAIS DOS DADOS #####
dados_filtrados['Financiado'] = dados_filtrados['Financiado'].astype(float).apply(regras.formata_float)
dados_filtrados['Liberado'  ] = dados_filtrados['Liberado'  ].apply(regras.formata_float)
dados_filtrados['Parcela'   ] = dados_filtrados['Parcela'   ].apply(regras.formata_float)
dados_filtrados['Comissão'  ] = dados_filtrados['Comissão'  ].apply(regras.formata_float)

dados_filtrados['Data da Mensagem' ] = pd.to_datetime(dados_filtrados['Data da Mensagem' ]).dt.strftime('%d/%m/%Y')
# dados_filtrados['Data da Liberação'] = pd.to_datetime(dados_filtrados['Data da Liberação']).dt.strftime('%d/%m/%Y')

df_controle = dados_filtrados.copy()

df_controle['Liberado'] = np.where(df_controle['Liberado'].empty, '0,00', df_controle['Liberado'])
df_controle['Comissão'] = np.where(df_controle['Comissão'].empty, '0,00', df_controle['Comissão'])

df_controle['Liberado'] = df_controle['Liberado'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
df_controle['Comissão'] = df_controle['Comissão'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)

df_controle['valor_disparos'] = np.where(df_controle['mensagens'] == 'Disparos', 0.1, 0)

controle = df_controle.groupby(['mensagens', 'Data da Mensagem']).agg({'numero': 'count', 'Data da Liberação': 'count', 'Liberado': 'sum', 'Comissão': 'sum', 'valor_disparos': 'sum'}).reset_index()

controle = controle.rename(columns={'mensagens': 'Campanhas', 'numero': 'leads', 'Data da Liberação': 'Pagos', 'Liberado': 'Valor de Produção', 'Comissão': 'Comissão Recebida'})

custo_campanhas = custo_campanhas.rename(columns={'data': 'Data da Mensagem', 'nome': 'Campanhas', 'leads': 'Leads', 'valor': 'Investimento'})

custo_campanhas['Data da Mensagem'] = pd.to_datetime(custo_campanhas['Data da Mensagem']).dt.strftime('%d/%m/%Y')

controle = pd.merge(controle, custo_campanhas, on=['Data da Mensagem', 'Campanhas'], how='outer')

controle['Investimento'] = np.where(controle['Campanhas'] == 'Disparos', controle['valor_disparos'], controle['Investimento'])

controle['Leads'] = np.where(controle['Leads'].isna(), controle['leads'], controle['Leads'])

controle = controle.groupby(['Campanhas']).sum().reset_index()

controle = controle[['Campanhas', 'Leads', 'Pagos', 'Valor de Produção', 'Comissão Recebida', 'Investimento']]

controle

media = (
    controle
    .groupby('Campanhas', as_index=False)
    .agg(
        valor_total_produzido=('Valor de Produção', 'sum'),
        valor_total_investido=('Investimento', 'sum'),
        valor_total_comissao=('Comissão Recebida', 'sum'),
        total_pago=('Pagos', 'sum'),
        total_leads=('Leads', 'sum')
    )
)

controle['Ticket Médio'] = (
    media['valor_total_produzido'] / media['total_pago']
)

controle['CAC'] = (
    controle['Investimento']
    .div(controle['Pagos'])
    .replace([np.inf, -np.inf], 0)
    .fillna(0)
)

controle['ROI'] = np.where(
    controle['Investimento'] == 0,
    1,
    (media['valor_total_comissao'] - media['valor_total_investido']) /
    media['valor_total_investido']
)

controle['ROI'] = np.where(controle['ROI'] >= 0, ['🟢 +' + regras.formata_float(x) for x in controle['ROI']], ['🔴 ' + regras.formata_float(x) for x in controle['ROI']])

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

controle['Valor de Produção'] = controle['Valor de Produção'].apply(regras.formata_float)
controle['Comissão Recebida'] = controle['Comissão Recebida'].apply(regras.formata_float)
controle['Ticket Médio'     ] = controle['Ticket Médio'     ].apply(regras.formata_float)
controle['Investimento'     ] = controle['Investimento'     ].apply(regras.formata_float)
controle['CAC'              ] = controle['CAC'              ].apply(regras.formata_float)


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


# ===============================================================
# Garantir tipos corretos
df_grafico = controle.copy()
df_grafico['ROI'] = df_grafico['ROI'].apply(regras.remover_emojis).replace('+', '')

df_grafico = df_grafico.rename(columns={
    "Valor de Produção": "valor_producao",
    "Comissão Recebida": "comissao_recebida",
    "Ticket Médio": "ticket_medio"
})

cols_float = [
    "valor_producao", "comissao_recebida",
    "Investimento", "ticket_medio", "CAC", "ROI"
]

for c in cols_float:
    df_grafico[c] = (
        df_grafico[c]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )

df_grafico["Leads"] = df_grafico["Leads"].astype(int)
df_grafico["Pagos"] = df_grafico["Pagos"].astype(int)

campanha_select = alt.selection_point(
    fields=["Campanhas"],
    empty="all"
)

graf_producao = (
    alt.Chart(df_grafico)
    .mark_bar()
    .encode(
        y=alt.Y(
            "Campanhas:N",
            sort="-x",
            title="Campanhas"
        ),
        x=alt.X(
            "valor_producao:Q",
            title="Valor de Produção (R$)"
        ),
        color=alt.condition(
            campanha_select,
            alt.value("#1f77b4"),
            alt.value("#d3d3d3")
        ),
        tooltip=[
            "Campanhas",
            alt.Tooltip("valor_producao:Q", format=",.2f"),
            alt.Tooltip("comissao_recebida:Q", format=",.2f")
        ]
    )
    .add_params(campanha_select)
    .properties(
        height=400,
        title="Valor de Produção por Campanha"
    )
)

graf_scatter = (
    alt.Chart(df_grafico)
    .mark_circle(size=120)
    .encode(
        x=alt.X("Investimento:Q", title="Investimento (R$)"),
        y=alt.Y("comissao_recebida:Q", title="Comissão Recebida (R$)"),
        size=alt.Size("Pagos:Q", title="Pagos"),
        color=alt.Color(
            "ROI:Q",
            scale=alt.Scale(scheme="redyellowgreen"),
            title="ROI"
        ),
        tooltip=[
            "Campanhas",
            alt.Tooltip("Investimento:Q", format=",.2f"),
            alt.Tooltip("comissao_recebida:Q", format=",.2f"),
            "Pagos",
            "ROI"
        ]
    )
    .properties(
        height=350,
        title="Eficiência: Investimento x Comissão"
    )
    .add_params(campanha_select)
    .transform_filter(campanha_select)
)

df_long = df_grafico.melt(
    id_vars="Campanhas",
    value_vars=["Leads", "Pagos"],
    var_name="Tipo",
    value_name="Quantidade"
)

base = (
    alt.Chart(df_long)
    .transform_calculate(
        qtd_pagos="""
        datum.Tipo === 'Leads' ? datum.Quantidade : null
        """
    )
    .transform_joinaggregate(
        producao='sum(qtd_pagos)',
        groupby=['Campanhas']
    )
    .transform_calculate(
        percentual="datum.Quantidade / datum.producao"
    )
)

graf_leads = (
    base
    .mark_bar()
    .encode(
        x=alt.X("Quantidade:Q", title="Quantidade"),
        y=alt.Y("Campanhas:N", sort="-x"),
        color=alt.Color("Tipo:N"),
        tooltip=[
            "Campanhas",
            "Tipo",
            alt.Tooltip("Quantidade:Q", title="Quantidade", format=","),
            alt.Tooltip("percentual:Q", title="Percentual", format=".2%")
        ]
    )
    .properties(
        height=350,
        title="Leads x Pagos"
    )
    .add_params(campanha_select)
    .transform_filter(campanha_select)
)

df_fin = df_grafico.melt(
    id_vars="Campanhas",
    value_vars=["comissao_recebida", "valor_producao"],
    var_name="Tipo",
    value_name="Valor"
)

base = (
    alt.Chart(df_fin)
    .transform_calculate(
        valor_producao_calc="""
        datum.Tipo === 'valor_producao' ? datum.Valor : null
        """
    )
    .transform_joinaggregate(
        producao='sum(valor_producao_calc)',
        groupby=['Campanhas']
    )
    .transform_calculate(
        percentual="datum.Valor / datum.producao"
    )
)

graf_fin = (
    alt.Chart(df_fin)
    .transform_joinaggregate(
        producao='sum(Valor)',
        groupby=['Campanhas']
    )
    .transform_calculate(
        percentual="""
        datum.Tipo === 'valor_producao'
            ? 1
            : datum.Valor / datum.producao
        """,
        ordem_stack="""
        datum.Tipo === 'valor_producao' ? 0 : 1
        """
    )
    .mark_bar()
    .encode(
        x=alt.X("Valor:Q", stack=True, title="Valor (R$)"),
        y=alt.Y("Campanhas:N", sort="-x"),
        color=alt.Color("Tipo:N", 
                        title="Tipo",
                        sort=["valor_producao", "comissao_recebida"]
        ),
        # 🔥 CONTROLE REAL DO STACK
        order=alt.Order("ordem_stack:Q", sort="ascending"),
        tooltip=[
            "Campanhas",
            "Tipo",
            alt.Tooltip("Valor:Q", format=",.2f"),
            alt.Tooltip("percentual:Q", format=".1%")
        ]
    )
    .properties(
        height=350,
        title="Produção x Comissão (% sobre Produção)"
    )
)


dashboard = alt.vconcat(
    graf_producao,
    alt.hconcat(
        graf_scatter,
        graf_leads
    ),
    graf_fin
).resolve_scale(color="independent")
# chart = (
#     alt.Chart(df_grafico)
#     .mark_bar()
#     .encode(
#         y=alt.Y("Campanhas:N", sort="-x"),
#         x=alt.X("valor_producao:Q", title="Valor de Produção (R$)"),
#         tooltip=[
#             "Campanhas",
#             alt.Tooltip("valor_producao:Q", format=",.2f")
#         ]
#     )
#     .properties(height=400)
# )

# st.altair_chart(chart, use_container_width=True)
# ===============================================================

##### ÁREA DA TABELA #####

if seleciona_exibicao == 'Planilha':
    with st.container():
        st.dataframe(controle, width='stretch', height=500, hide_index=True)
else:
    with st.container():
        st.altair_chart(graf_producao, use_container_width=True)
    with st.container():
        st.altair_chart(graf_scatter, use_container_width=True)
    with st.container():
        st.altair_chart(graf_leads, use_container_width=True)
    with st.container():
        st.altair_chart(graf_fin, use_container_width=True)

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

