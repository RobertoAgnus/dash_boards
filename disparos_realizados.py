import streamlit as st
import pandas as pd
import itertools
import altair as alt
from datetime import date, datetime

from querys.querys_sql import QuerysSQL
from querys.connect import Conexao


##### CONFIGURAÇÃO DA PÁGINA #####
st.set_page_config(
    page_title="Disparos Realizados",
    page_icon="image/logo_agnus.ico",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

##### CRIAR INSTÂNCIA DAS QUERYS #####
consulta_sql = QuerysSQL()

##### CACHE DE CONSULTAS #####
@st.cache_data(show_spinner=False)
def carregar_dados():
    conectar = Conexao()
    conectar.conectar_postgres()
    conn = conectar.obter_conexao_postgres()

    disparos = consulta_sql.disparos()
    df = pd.read_sql_query(disparos, conn)

    conectar.desconectar_postgres()

    return df

##### FUNÇÃO PARA OBTER AS DATAS #####
def get_datas(dados):
    df_datas = dados['data']
    
    # Converting the 'data' column to datetime format (caso não esteja)
    df_datas['data'] = pd.to_datetime(df_datas, dayfirst=True)

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = df_datas['data'].min()
    maior_data = date.today()

    return menor_data, maior_data

def get_qtd_disparos(dados, selectbox_status, intervalo):
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
    dados['data'] = pd.to_datetime(dados['data'], dayfirst=True)

    if selectbox_status == 'Selecionar':
        condicao = (dados['data'] >= data_inicio) & (dados['data'] <= data_fim)
    else:
        condicao = (dados['status'].isin(selectbox_status)) & ((dados['data'] >= data_inicio) & (dados['data'] <= data_fim))

    qtd_disparos = dados[condicao]
    qtd_disparos = qtd_disparos.groupby(["data","telefone","CPF"]).count()

    return qtd_disparos

def get_total_disparos(dados, intervalo):
    if len(intervalo) == 2:
        data_inicio, data_fim = intervalo
    
    elif len(intervalo) == 1:
        data_inicio = intervalo[0]
        data_fim = datetime.strptime('2030-12-31', "%Y-%m-%d")
    
    else:
        data_inicio, data_fim = get_datas(dados)

    data_inicio = pd.to_datetime(data_inicio)
    data_fim = pd.to_datetime(data_fim)

    disparos_por_data = dados[(dados['data'] >= data_inicio) & (dados['data'] <= data_fim)]
    qtd_disparos = len(disparos_por_data)
    
    return qtd_disparos

def get_disparos(dados, selectbox_status, selectbox_disparos, intervalo):
    def filtrar_data():
        if len(intervalo) == 2:
            data_inicio, data_fim = intervalo
        
        elif len(intervalo) == 1:
            data_inicio = intervalo[0]
            data_fim = datetime.strptime('2030-12-31', "%Y-%m-%d")
        
        else:
            data_inicio, data_fim = get_datas(dados)

        data_inicio = pd.to_datetime(data_inicio)
        data_fim = pd.to_datetime(data_fim)

        return data_inicio, data_fim

    df_disparados_2 = dados.groupby(['data','telefone','CPF']).size().reset_index(name="Qtd")
    df_disparados = pd.merge(dados,df_disparados_2, on=['data','telefone','CPF'], how='left')

    if selectbox_disparos != 'Selecionar' and selectbox_status != 'Selecionar':
        df_disparados = df_disparados[(df_disparados['Qtd'] == selectbox_disparos) & (df_disparados['status'].isin(selectbox_status))]
    elif selectbox_disparos == 'Selecionar' and selectbox_status != 'Selecionar':
        df_disparados = df_disparados[(df_disparados['status'].isin(selectbox_status))]
    elif selectbox_disparos != 'Selecionar' and selectbox_status == 'Selecionar':
        df_disparados = df_disparados[(df_disparados['Qtd'] == selectbox_disparos)]

    data_inicio, data_fim = filtrar_data()

    df_disparados = df_disparados[(df_disparados['data'] >= data_inicio) & (df_disparados['data'] <= data_fim)]

    df_disparados = df_disparados[['data','telefone','CPF','status', 'vendedor']].sort_values(['data','CPF'], ascending=[False, True])

    df_disparos_agrupados = df_disparados.groupby(['data', 'status']).size().reset_index(name='Qtd')
    df_disparos_agrupados['data'] = pd.to_datetime(df_disparos_agrupados['data'],format="%d/%m/%Y")

    if df_disparos_agrupados.empty:
        data_inicio, data_fim = filtrar_data()

        datas = pd.date_range(start=data_inicio, end=data_fim)

        status = df_disparos_agrupados['status'].unique() if not df_disparos_agrupados.empty else ['CLOSE', 'DESCONHECIDO', 'ERRO', 'PENDING', 'pendente']
        
        combinacoes = list(itertools.product(datas, status))

        # Cria o DataFrame com Qtd zerada
        df_disparos_agrupados = pd.DataFrame(combinacoes, columns=['data', 'status'])
        df_disparos_agrupados['Qtd'] = 0

    return df_disparados, df_disparos_agrupados

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

dados = carregar_dados()

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE STATUS #####
    status = dados['status']

    if "filtro_status" not in st.session_state:
        st.session_state.filtro_status = "Selecionar"

    # Adiciona selectbox status na sidebar:
    selectbox_status = st.multiselect(
        'Selecione o Status do Atendimento',
        ["Selecionar"] + status.unique().tolist(),
        key="filtro_status"
    )

    ##### FILTRO DE INTERVALO DE DATA #####
    menor_data, maior_data = get_datas(dados)
    
    if "filtro_periodo" not in st.session_state:
        st.session_state.filtro_periodo = (menor_data, maior_data)

    intervalo = st.date_input(
        "Selecione um intervalo de datas:",
        value=(menor_data,maior_data),
        key="filtro_periodo"
    )

    ##### QUANTIDADE DE DISPAROS #####
    qtd_disparos = get_qtd_disparos(dados, selectbox_status, intervalo)

    if "filtro_disparos" not in st.session_state:
        st.session_state.filtro_disparos = "Selecionar"

    selectbox_disparos = st.selectbox(
        'Selecione a Quantidade de Disparos',
        ["Selecionar"] + qtd_disparos['status'].unique().tolist(),
        key="filtro_disparos"
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
        st.title(":blue[Análise dos Disparos]")

df_disparos, df_disparos_agrupados = get_disparos(dados, selectbox_status, selectbox_disparos, intervalo)

##### ÁREA DOS CARDS #####
with st.container():
    col_1, col_2, col_3 = st.columns((2.5,2.5,2.5))

    ##### TOTAL DE DISPAROS #####
    with col_1:
        df_total_disparos = get_total_disparos(dados, intervalo)

        metric_card("Total de Disparos", f"{format(int(df_total_disparos), ',').replace(',', '.')}")

    ##### DEISPAROS POR STATUS #####
    with col_2:
        
        texto = ''
        if selectbox_status == 'Selecionar':
            df_disparos_status = df_disparos
            texto = 'Todos'
        else:    
            df_disparos_status = df_disparos[df_disparos['status'].isin(selectbox_status)]
            texto = selectbox_status

        metric_card(f'Disparos por Status', f"{format(int(df_disparos_status['status'].shape[0]), ',').replace(',', '.')}")

    ##### % DE DISPAROS POR STATUS DO TOTAL #####
    with col_3:
        valor = f"{(int(df_disparos_status['status'].shape[0]) / int(df_total_disparos) * 100):.2f}".replace('.',',')
        metric_card(f'% do Total', f'{valor} %')

    
##### CORPO DO DASHBOARD #####
with st.container():
    col_1, col_2 = st.columns((5, 5), gap="medium")

    with col_1:
        st.markdown("### :blue[Status por Data]")

        df_disparos_agrupados['data'] = pd.to_datetime(df_disparos_agrupados['data']).dt.date

        chart = (
            alt.Chart(df_disparos_agrupados)
            .mark_line(point=True)
            .encode(
                x=alt.X(
                    'data:T',
                    title='Data',
                    axis=alt.Axis(format='%d/%m/%Y')
                ),
                y=alt.Y(
                    'Qtd:Q',
                    title='Quantidade',
                    scale=alt.Scale(domain=[0, df_disparos_agrupados['Qtd'].max() * 1.02])
                ),
                color='status:N',
                tooltip=['data', 'status', 'Qtd']
            )
            .properties(
                height=500
            )
        )

        st.altair_chart(chart, use_container_width=True)

    with col_2:
        ##### TABELA DE CLIENTES #####
        df_disparos['data'] = pd.to_datetime(df_disparos['data'])
        df_disparos['data'] = df_disparos['data'].dt.strftime('%d/%m/%Y')
        
        st.markdown("### :blue[Disparos por Clientes]")
        st.dataframe(df_disparos, height=500, hide_index=True)

        ##### BOTÃO EXPORTAR TABELA #####
        csv = df_disparos.to_csv(index=False)
        st.download_button(
            label="⬇️ Baixar planilha",
            data=csv,
            file_name="dados.csv",
            mime="text/csv",
        )
