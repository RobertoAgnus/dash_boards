import streamlit as st
import altair as alt
import pandas as pd
import itertools
from datetime import date, datetime

from querys.querys_sql import QuerysSQL
from querys.connect import Conexao

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
    conn = conectar.obter_conexao_mysql()
    
    clientes_atendidos = consulta.clientes_atendidos_v1()
    df = pd.read_sql(clientes_atendidos, conn)

    qtd_total_clientes = consulta.total_clientes()
    df_total_clientes = pd.read_sql(qtd_total_clientes, conn)

    mapeamento_etapa = {
                            "ABERTURA": ["ABERTURA"],
                            "ANIVERSARIANTE": ["ANIVERSARIANTE"],
                            "CHAVE_PIX": ["CHAVE_PIX"],
                            "CLT": ["CLT"],
                            "COM_SALDO": ["COM_SALDO"],
                            "CONSULTAR": ["CONSULTAR"],
                            "CONTRATO_DIGITADO": ["CONTRATO_DIGITADO"],
                            "DESCONHECIDO": ["DESCONHECIDO"],
                            "DISPARO": ["DISPARO"],
                            "FINALIZADO": ["FINALIZADO"],
                            "INSTABILIDADE": ["INSTABILIDADE"],
                            "OPORTUNIDADES": ["OPORTUNIDADES"],
                            "SIMULANDO": ["SIMULANDO"],
                            "TENTAR": ["TENTAR"],
                            "NAO_AUTORIZADO": ["NAO_AUTORIZADO","NAO_AUTORIZADO_III","NAO_AUTORIZADO_IIII","POLITICA_INTERNA","SEM_CONTA","SEM_SALDO","FORA_MODALIDADE"],
                            "CPF_INVALIDO": ["CPF_INVALIDO_II", "CPF_INVALIDO_III"],
                            "LEAD_NOVO": ["LEAD_NOVO", "LEAD_NOVO_II", "LEAD_NOVO_III", "LEAD_NOVO_IIII", "LEAD_NOV0_IIII"],
                            "MUDANCAS_CADASTRAIS": ["MUDANCAS_CADASTRAIS", "MUDANCA_CADASTRAIS"]
                        }
    
    mapa_invertido = {
        variante: chave
        for chave, variantes in mapeamento_etapa.items()
        for variante in variantes
    }

    # Criar nova coluna
    df["etapa_padronizada"] = df["Etapa"].map(mapa_invertido).fillna(df["Etapa"])

    conectar.desconectar_mysql()

    return {"df": df, "total_clientes": df_total_clientes}


##### CARREGAR OS DADOS (1x) #####
dicionario = carregar_dados()

dados = dicionario['df']
qtd_total_clientes = dicionario['total_clientes']


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

##### FUNÇÃO PARA OBTER AS ETAPAS #####
def get_etapas(dados):
    etapas = dados['etapa_padronizada'].unique()
    etapas = [x for x in etapas if x is not None]
    
    return sorted(list(set(etapas)))

##### FUNÇÃO PARA OBTER AS DATAS #####
def get_datas(dados):
    df_datas = dados['Data']
    
    # Converting the 'data' column to datetime format (caso não esteja)
    df_datas['Data'] = pd.to_datetime(df_datas, dayfirst=True)

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = df_datas['Data'].min()
    maior_data = date.today()

    return menor_data, maior_data

##### FUNÇÃO PARA OBTER OS CONTATOS REALZIADOS #####
def get_contatos_realizados(dados, selectbox_etapa, intervalo):
    if len(intervalo) == 2:
        data_inicio, data_fim = intervalo
    
    elif len(intervalo) == 1:
        data_inicio = intervalo[0]
        data_fim = datetime.strptime('2030-12-31', "%Y-%m-%d")
    
    else:
        data_inicio, data_fim = get_datas(dados)

    data_inicio = pd.to_datetime(data_inicio)
    data_fim = pd.to_datetime(data_fim)

    # Converte a coluna Data para datetime
    dados['Data'] = pd.to_datetime(dados['Data'], dayfirst=True)

    if selectbox_etapa == 'Selecionar':
        condicao = (dados['Data'] >= data_inicio) & (dados['Data'] <= data_fim)
    else:
        condicao = (dados['etapa_padronizada'] == selectbox_etapa) & ((dados['Data'] >= data_inicio) & (dados['Data'] <= data_fim))

    df_clientes_atendidos = dados[condicao]
    
    return df_clientes_atendidos

##### FUNÇÃO PARA PREPAR OS DADOS PARA O GRÁFICO #####
def get_etapa_por_data(dados, etapa, intervalo):
    if len(intervalo) == 2:
        data_inicio, data_fim = intervalo
    
    elif len(intervalo) == 1:
        data_inicio = intervalo[0]
        data_fim = datetime.strptime('2030-12-31', "%Y-%m-%d")
    
    else:
        data_inicio, data_fim = get_datas(dados)

    data_inicio = pd.to_datetime(data_inicio)
    data_fim = pd.to_datetime(data_fim)

    dados_agrupados = dados.groupby(['Data', 'etapa_padronizada']).size().reset_index(name='Qtd')
    dados_agrupados['Data'] = pd.to_datetime(dados_agrupados['Data'],format="%d/%m/%Y")

    if etapa == 'Selecionar':
        condicao = (dados_agrupados['Data'] >= data_inicio) & (dados_agrupados['Data'] <= data_fim)
    else:
        condicao = (dados_agrupados['etapa_padronizada'] == etapa) & ((dados_agrupados['Data'] >= data_inicio) & (dados_agrupados['Data'] <= data_fim))

    dados_agrupados = dados_agrupados[condicao]

    if dados_agrupados.empty:
        datas = pd.date_range(start=data_inicio, end=data_fim)
        etapa = dados_agrupados['etapa_padronizada'].unique() if not dados_agrupados.empty else ["ABERTURA","ANIVERSARIANTE","CHAVE_PIX","CLT","COM_SALDO","CONSULTAR","CONTRATO_DIGITADO","DESCONHECIDO","DISPARO","FINALIZADO","FORA_MODALIDADE","INSTABILIDADE","NAO_AUTORIZADO","CPF_INVALIDO","LEAD_NOVO","MUDANCAS_CADASTRAIS"]
        
        combinacoes = list(itertools.product(datas, etapa))

        # Cria o DataFrame com Qtd zerada
        dados_agrupados = pd.DataFrame(combinacoes, columns=['Data', 'Etapa'])
        dados_agrupados['Qtd'] = 0
        
    return dados_agrupados.rename(columns={'etapa_padronizada': 'Etapa'})
    

##### ÁREA DO DASHBOARD #####

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE Etapa #####
    etapa = get_etapas(dados)
    
    # Adiciona selectbox etapa na sidebar:
    selectbox_etapa = st.selectbox(
        'Selecione a Etapa do Atendimento',
        ["Selecionar"] + etapa,
        index=0
    )

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data, maior_data = get_datas(dados)

    ##### FILTRO DE INTERVALO DE DATA #####
    intervalo = st.date_input(
        "Selecione um intervalo de datas:",
        value=(menor_data,maior_data)
    )


##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1a, col_2a = st.columns((2, 8))

    with col_1a:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2a:
        st.title(":blue[Análise dos Clientes]")

##### CORPO DO DASHBOARD #####
with st.container():
    col_1b, col_2b, col_3b = st.columns((3.3, 3.3,3.3))

    ##### ÁREA DOS CARDS #####
    with col_1b:
        ##### CARD TOTAL CLIENTES ÚNICOS #####
        metric_card("Total de Clientes únicos", f"{format(int(qtd_total_clientes['TOTAL_CPF']), ',').replace(',', '.')}")

    with col_2b:                    
        ##### CARD CONTATOS REALIZADOS #####
        df_clientes_atendidos = get_contatos_realizados(dados, selectbox_etapa, intervalo)
            
        metric_card(f'Contatos realizados "{"todos" if selectbox_etapa == "Selecionar" else selectbox_etapa}"', f"{format(int(df_clientes_atendidos.shape[0]), ',').replace(',', '.')}")
        
    with col_3b:
        ##### CARD % DE ATENDIMENTOS DO TOTAL #####
        valor = f"{(int(df_clientes_atendidos.shape[0]) / int(qtd_total_clientes['TOTAL_CPF']) * 100):.2f}".replace('.',',')
        metric_card("% de atendimentos do Total", f"{valor} %")

##### ÁREA DO GRÁFICO E DA TABELA #####
with st.container():
    col_1c, col_2c = st.columns((5,5))

    with col_1c:
        ##### GRÁFICO DE ETAPA POR DATA #####
        st.markdown("### :blue[Etapa por Data]")

        df_clientes_atendidos_agrupados = get_etapa_por_data(dados, selectbox_etapa, intervalo)

        chart = (
            alt.Chart(df_clientes_atendidos_agrupados)
            .mark_line(point=True)
            .encode(
                x=alt.X(
                    'Data:T',
                    title='Data',
                    axis=alt.Axis(format='%d/%m/%Y')
                ),
                y=alt.Y(
                    'Qtd:Q',
                    title='Quantidade',
                    scale=alt.Scale(domain=[0, df_clientes_atendidos_agrupados['Qtd'].max() * 1.02])
                ),
                color='Etapa:N',
                tooltip=['Data', 'Etapa', 'Qtd']
            )
            .properties(
                height=500
            )
        )

        st.altair_chart(chart, use_container_width=True)

    with col_2c:
        ##### TABELA DE CLIENTES #####
        st.markdown("### :blue[Detalhamento dos Clientes]")

        df_clientes_atendidos = df_clientes_atendidos[['Data', 'CPF', 'Nome', 'telefoneLead', 'Cidade', 'UF', 'etapa_padronizada']]
        df_clientes_atendidos = df_clientes_atendidos.rename(columns={'telefoneLead': 'Telefone', 'etapa_padronizada': 'Etapa'})
        st.dataframe(df_clientes_atendidos, width='stretch', height=500, hide_index=True)

        ##### BOTÃO EXPORTAR TABELA #####
        csv = df_clientes_atendidos.to_csv(index=False)
        st.download_button(
            label="⬇️ Baixar planilha",
            data=csv,
            file_name="dados.csv",
            mime="text/csv",
        )