import streamlit as st
import altair as alt
import pandas as pd
import numpy as np
import itertools
from datetime import date, datetime, timedelta
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
    
    consulta_crm = consulta.get_crm()
    df_crm = pd.read_sql(consulta_crm, conn_mysql)
    df_crm['cpf'] = (
        df_crm['cpf']
        .astype(str)
        .str.replace(r'\D', '', regex=True)  # remove tudo que não é dígito
        .str.strip()                         # remove espaços
    )

    # mantém só CPFs com 11 dígitos
    df_crm.loc[~df_crm['cpf'].str.fullmatch(r'\d{11}'), 'cpf'] = None
    df_crm['dataConsulta'] = pd.to_datetime(df_crm['dataConsulta']).dt.date

    consulta_digisac = consulta.get_digisac()
    df_digisac = pd.read_sql_query(consulta_digisac, conn_postgres)
    # df_digisac['data_digisac'] = pd.to_datetime(df_digisac['data']).dt.strftime('%d/%m/%Y')

    consulta_corban = consulta.get_corban()
    df_corban = pd.read_sql_query(consulta_corban, conn_postgres)
    df_corban['cpf'] = df_corban['cpf'].astype(str).str.zfill(11)
    df_corban['data_atualizacao_api'] = pd.to_datetime(df_corban['data_atualizacao_api']).dt.date
    # df_corban['data_api'] = pd.to_datetime(df_corban['data_atualizacao_api']).dt.strftime('%d/%m/%Y')

    df1 = pd.merge(df_crm, df_digisac, left_on=['cpf'], right_on=['cpf'], how='left')
    df = pd.merge(df1, df_corban, left_on=['cpf'], right_on=['cpf'], how='left')

    # df = df[~df['data'].isnull()]

    df['col_aux1'] = np.where(df['telefone_x'].notna() & (df['telefone_x'] != ''), df['telefone_x'], df['telefoneLead'])
    df['col_aux2'] = np.where(df['col_aux1'].notna() & (df['col_aux1'] != ''), df['col_aux1'], df['telefone_y'])
    df['telefone'] = np.where(df['col_aux2'].notna() & (df['col_aux2'] != ''), df['col_aux2'], df['numero'])

    df = df[['dataConsulta', 'cpf', 'nome', 'telefone', 'erros', 'tabela', 'data', 'parcelas', 'valorLiberado', 'valorContrato', 'falha', 'data_atualizacao_api', 'status_api']]

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
                    'data_atualizacao_api': 'Data Croban',
                    'status_api': 'Status Corban'
                }
    
    df = df.rename(columns=renomear)

    return df.drop_duplicates()
    # return df_digisac.drop_duplicates()
    # return df_corban.drop_duplicates()


##### CARREGAR OS DADOS (1x) #####
dados = carregar_dados()

print(dados.head())


# dados = dicionario['df']
# qtd_total_clientes = dicionario['total_clientes']


# ##### FUNÇÃO PARA GERAR OS CARDS #####
# def metric_card(label, value):
#     st.markdown(
#         f"""
#         <div style="
#             background-color: #262730;
#             border-radius: 10px;
#             text-align: center;
#             margin-bottom: 15px;
#             height: auto;
#         ">
#             <p style="color: white; font-weight: bold;">{label}</p>
#             <h3 style="color: white; font-size: calc(1rem + 1vw)">{value}</h3>
#         </div>
#         """,
#         unsafe_allow_html=True
#     )

# ##### FUNÇÃO PARA OBTER AS ETAPAS #####
# def get_etapas(dados):
#     etapas = dados['etapa_padronizada'].unique()
#     etapas = [x for x in etapas if x is not None]
    
#     return sorted(list(set(etapas)))

# ##### FUNÇÃO PARA OBTER AS DATAS #####
# def get_datas(dados):
#     df_datas = dados['Data']
    
#     # Converting the 'data' column to datetime format (caso não esteja)
#     df_datas['Data'] = pd.to_datetime(df_datas, dayfirst=True)

#     # Obtendo a menor e a maior data da coluna 'data'
#     menor_data = df_datas['Data'].min()
#     maior_data = date.today()

#     return menor_data, maior_data

# ##### FUNÇÃO PARA OBTER OS CONTATOS REALZIADOS #####
# def get_contatos_realizados(dados, selectbox_etapa, intervalo):
#     if len(intervalo) == 2:
#         data_inicio, data_fim = intervalo
    
#     elif len(intervalo) == 1:
#         data_inicio = intervalo[0]
#         data_fim = datetime.strptime('2030-12-31', "%Y-%m-%d")
    
#     else:
#         data_inicio, data_fim = get_datas(dados)

#     data_inicio = pd.to_datetime(data_inicio)
#     data_fim = pd.to_datetime(data_fim)

#     # Converte a coluna Data para datetime
#     dados['Data'] = pd.to_datetime(dados['Data'], dayfirst=True)

#     if selectbox_etapa == 'Selecionar':
#         condicao = (dados['Data'] >= data_inicio) & (dados['Data'] <= data_fim)
#     else:
#         condicao = (dados['etapa_padronizada'].isin(selectbox_etapa)) & ((dados['Data'] >= data_inicio) & (dados['Data'] <= data_fim))

#     df_clientes_atendidos = dados[condicao]
    
#     return df_clientes_atendidos

# ##### FUNÇÃO PARA PREPAR OS DADOS PARA O GRÁFICO #####
# def get_etapa_por_data(dados, etapa, intervalo):
#     if len(intervalo) == 2:
#         data_inicio, data_fim = intervalo
    
#     elif len(intervalo) == 1:
#         data_inicio = intervalo[0]
#         data_fim = datetime.strptime('2030-12-31', "%Y-%m-%d")
    
#     else:
#         data_inicio, data_fim = get_datas(dados)

#     data_inicio = pd.to_datetime(data_inicio)
#     data_fim = pd.to_datetime(data_fim)

#     dados_agrupados = dados.groupby(['Data', 'etapa_padronizada']).size().reset_index(name='Qtd')
#     dados_agrupados['Data'] = pd.to_datetime(dados_agrupados['Data'],format="%d/%m/%Y")

#     if etapa == 'Selecionar':
#         condicao = (dados_agrupados['Data'] >= data_inicio) & (dados_agrupados['Data'] <= data_fim)
#     else:
#         condicao = (dados_agrupados['etapa_padronizada'].isin(etapa)) & ((dados_agrupados['Data'] >= data_inicio) & (dados_agrupados['Data'] <= data_fim))

#     dados_agrupados = dados_agrupados[condicao]

#     if dados_agrupados.empty:
#         datas = pd.date_range(start=data_inicio, end=data_fim)
#         etapa = dados_agrupados['etapa_padronizada'].unique() if not dados_agrupados.empty else ["ABERTURA","ANIVERSARIANTE","CHAVE_PIX","CLT","COM_SALDO","CONSULTAR","CONTRATO_DIGITADO","DESCONHECIDO","DISPARO","FINALIZADO","FORA_MODALIDADE","INSTABILIDADE","NAO_AUTORIZADO","CPF_INVALIDO","LEAD_NOVO","MUDANCAS_CADASTRAIS"]
        
#         combinacoes = list(itertools.product(datas, etapa))

#         # Cria o DataFrame com Qtd zerada
#         dados_agrupados = pd.DataFrame(combinacoes, columns=['Data', 'Etapa'])
#         dados_agrupados['Qtd'] = 0
        
#     return dados_agrupados.rename(columns={'etapa_padronizada': 'Etapa'})
    

##### ÁREA DO DASHBOARD #####

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE TELEFONES #####
    if "filtro_telefone" not in st.session_state:
        st.session_state.filtro_telefone = "Selecionar"

    lista_telefone = ["Todos", "Sem Telefone", "Com Telefone"]

    # Adiciona selectbox telefone na sidebar:
    selectbox_telefone = st.selectbox(
        'Selecione COM ou SEM Telefone',
        lista_telefone,
        key="filtro_telefone"
    )

    ##### FILTRO DE ERROS #####
    erros = dados['Retorno Consulta'].unique()
    erros = [x for x in erros if x is not None]

    if "filtro_erros" not in st.session_state:
        st.session_state.filtro_erros = "Selecionar"

    lista_erros = ["Selecionar"] + erros

    # Adiciona selectbox erros na sidebar:
    selectbox_erros = st.multiselect(
        'Selecione os Retornos desejados',
        lista_erros,
        key="filtro_erros"
    )

dados = dados
#     ##### FILTRO DE Etapa #####
#     etapa = get_etapas(dados)
    
#     if "filtro_etapa" not in st.session_state:
#         st.session_state.filtro_etapa = "Selecionar"

#     lista_etapa = ["Selecionar"] + etapa

#     # Adiciona selectbox etapa na sidebar:
#     selectbox_etapa = st.multiselect(
#         'Selecione a Etapa do Atendimento',
#         lista_etapa,
#         key="filtro_etapa"
#     )

#     ##### FILTRO DE INTERVALO DE DATA #####
#     # Obtendo a menor e a maior data da coluna 'data'
#     menor_data, maior_data = get_datas(dados)

#     if "filtro_periodo" not in st.session_state:
#         hoje = date.today()
#         st.session_state.filtro_periodo = (menor_data, hoje) 

#     intervalo = st.date_input(
#         "Selecione um intervalo de datas:",
#         value=(menor_data,maior_data),
#         key="filtro_periodo"
#     )

#     # Botão de limpeza
#     if st.button("🧹 Limpar filtros"):
#         st.session_state.clear()
#         st.rerun()

        

##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1a, col_2a = st.columns((2, 8))

    with col_1a:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2a:
        st.title(":blue[Análise dos Clientes]")

# ##### CORPO DO DASHBOARD #####
# with st.container():
#     col_1b, col_2b, col_3b = st.columns((3.3, 3.3,3.3))

#     ##### ÁREA DOS CARDS #####
#     with col_1b:
#         ##### CARD TOTAL CLIENTES ÚNICOS #####
#         metric_card("Total de Clientes únicos", f"{format(int(qtd_total_clientes['TOTAL_CPF']), ',').replace(',', '.')}")

#     with col_2b:                    
#         ##### CARD CONTATOS REALIZADOS #####
#         df_clientes_atendidos = get_contatos_realizados(dados, selectbox_etapa, intervalo)
            
#         metric_card(f'Contatos realizados', f"{format(int(df_clientes_atendidos.shape[0]), ',').replace(',', '.')}")
        
#     with col_3b:
#         ##### CARD % DE ATENDIMENTOS DO TOTAL #####
#         valor = f"{(int(df_clientes_atendidos.shape[0]) / int(qtd_total_clientes['TOTAL_CPF']) * 100):.2f}".replace('.',',')
#         metric_card("% de atendimentos do Total", f"{valor} %")

# ##### ÁREA DO GRÁFICO E DA TABELA #####
# with st.container():
#     col_1c, col_2c = st.columns((5,5))

#     with col_1c:
#         ##### GRÁFICO DE ETAPA POR DATA #####
#         st.markdown("### :blue[Etapa por Data]")

#         df_clientes_atendidos_agrupados = get_etapa_por_data(dados, selectbox_etapa, intervalo)

#         df_clientes_atendidos_agrupados['Data'] = pd.to_datetime(df_clientes_atendidos_agrupados['Data']).dt.date

#         chart = (
#             alt.Chart(df_clientes_atendidos_agrupados)
#             .mark_line(point=True)
#             .encode(
#                 x=alt.X(
#                     'Data:T',
#                     title='Data',
#                     axis=alt.Axis(format='%d/%m/%Y')
#                 ),
#                 y=alt.Y(
#                     'Qtd:Q',
#                     title='Quantidade',
#                     scale=alt.Scale(domain=[0, df_clientes_atendidos_agrupados['Qtd'].max() * 1.02])
#                 ),
#                 color='Etapa:N',
#                 tooltip=['Data', 'Etapa', 'Qtd']
#             )
#             .properties(
#                 height=500
#             )
#         )

#         st.altair_chart(chart, use_container_width=True)

#     with col_2c:
#         ##### TABELA DE CLIENTES #####
#         st.markdown("### :blue[Detalhamento dos Clientes]")

#         df_clientes_atendidos = df_clientes_atendidos[['Data', 'CPF', 'Nome', 'telefoneLead', 'Cidade', 'UF', 'etapa_padronizada']]
#         df_clientes_atendidos = df_clientes_atendidos.rename(columns={'telefoneLead': 'Telefone', 'etapa_padronizada': 'Etapa'})

#         df_clientes_atendidos['Data'] = pd.to_datetime(df_clientes_atendidos['Data'])
#         df_clientes_atendidos['Data'] = df_clientes_atendidos['Data'].dt.strftime('%d/%m/%Y')

#         st.dataframe(df_clientes_atendidos, width='stretch', height=500, hide_index=True)
st.dataframe(dados, width='stretch', height=500, hide_index=True)

##### BOTÃO EXPORTAR TABELA #####
dados_xlsx = dados[['Nome','Telefone']]
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