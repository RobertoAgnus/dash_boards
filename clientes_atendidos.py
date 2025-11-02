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

@st.cache_data
def get_total_clientes_unicos(total_clientes_unicos):
    conectar = Conexao()
    conectar.conectar_mysql()
    conn = conectar.obter_conexao_mysql()
    df = pd.read_sql(total_clientes_unicos, conn)

    conectar.desconectar_mysql()

    return df

@st.cache_data
def get_qtd_clientes_atendidos(qtd_clientes_atendidos):
    conectar = Conexao()
    conectar.conectar_mysql()
    conn = conectar.obter_conexao_mysql()
    df = pd.read_sql(qtd_clientes_atendidos, conn)

    conectar.desconectar_mysql()

    return df

@st.cache_data
def get_etapa_atendimento(etapa_atendimento):
    conectar = Conexao()
    conectar.conectar_mysql()
    conn = conectar.obter_conexao_mysql()
    df = pd.read_sql(etapa_atendimento, conn)

    conectar.desconectar_mysql()

    return df

@st.cache_data
def get_datas(datas):
    conectar = Conexao()
    conectar.conectar_mysql()
    conn = conectar.obter_conexao_mysql()
    df = pd.read_sql(datas, conn)

    conectar.desconectar_mysql()

    return df

##### BARRA LATERAL #####
with st.sidebar:
    st.title('Filtros')

    ##### FILTRO DE Etapa #####
    etapa_atendimento = consulta.etapa_atendimentos()
    etapa = get_etapa_atendimento(etapa_atendimento)
    etapa = etapa[etapa['Etapa'].isin(['LEAD_NOVO', 'SEM_SALDO', 'NAO_AUTORIZADO', 'COM_SALDO'])]

    # Adiciona selectbox etapa na sidebar:
    selectbox_etapa = st.selectbox(
        'Selecione a Etapa do Atendimento',
        ["Selecionar"] + etapa['Etapa'].unique().tolist(),
        index=0
    )

    datas = consulta.datas_atendimentos()
    df_datas = get_datas(datas)
    
    # Converting the 'data' column to datetime format (caso não esteja)
    df_datas['data'] = pd.to_datetime(df_datas['data'], dayfirst=True)

    # Obtendo a menor e a maior data da coluna 'data'
    menor_data = df_datas['data'].min()
    maior_data = date.today()

    ##### FILTRO DE INTERVALO DE DATA #####
    intervalo = st.date_input(
        "Selecione um intervalo de datas:",
        value=(menor_data,maior_data)
    )

    # Trata o intervalo de data para busca no banco de dados
    if len(intervalo) == 2:
        data_inicio, data_fim = intervalo
        intervalo_data = f"between '{data_inicio}' and '{data_fim}'"
    elif len(intervalo) == 1:
        data_inicio = intervalo[0]
        data_fim = datetime.strptime('2030-12-31', "%Y-%m-%d")
        intervalo_data = f"between '{data_inicio}' and '{data_fim}'"
    else:
        intervalo_data = f"between '{menor_data}' and '{maior_data}'"

##### TÍTULO DO DASHBOARD #####
with st.container():
    col_1, col_2 = st.columns((2, 8))

    with col_1:
        st.image("image/logo_agnus.jpg", width=200)
    with col_2:
        st.title(":blue[Análise dos Clientes]")

##### CORPO DO DASHBOARD #####
with st.container():
    col_2a, col_3a, col_4a = st.columns((3.3, 3.3,3.3))
    
    ##### ÁREA DOS CARDS #####
    # with col_1a:
    #     st.markdown("### :blue[Clientes Atendidos]")

    with col_2a:
        ##### CARD TOTAL CLIENTES ÚNICOS #####
        total_clientes_unicos = consulta.total_clientes()
        df_total_clientes_unicos = get_total_clientes_unicos(total_clientes_unicos)
        
        metric_card("Total de Clientes únicos", f"{format(int(df_total_clientes_unicos['TOTAL_CPF']), ',').replace(',', '.')}")

    with col_3a:                    
        ##### CARD CONTATOS REALIZADOS #####
        qtd_clientes_atendidos = consulta.clientes_atendidos(selectbox_etapa, intervalo_data)
        df_clientes_atendidos = get_qtd_clientes_atendidos(qtd_clientes_atendidos)
        df_clientes_atendidos_card = df_clientes_atendidos.drop_duplicates(subset=['CPF','Etapa'])

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
        df_clientes_atendidos["etapa_padronizada"] = df_clientes_atendidos["Etapa"].map(mapa_invertido).fillna(df_clientes_atendidos["Etapa"])
        
        df_clientes_atendidos_agrupados = df_clientes_atendidos.groupby(['Data', 'etapa_padronizada']).size().reset_index(name='Qtd')
        df_clientes_atendidos_agrupados['Data'] = pd.to_datetime(df_clientes_atendidos_agrupados['Data'],format="%d/%m/%Y")

        if df_clientes_atendidos_agrupados.empty:
            datas = pd.date_range(start=data_inicio, end=data_fim)
            etapa = df_clientes_atendidos_agrupados['etapa_padronizada'].unique() if not df_clientes_atendidos_agrupados.empty else ["ABERTURA","ANIVERSARIANTE","CHAVE_PIX","CLT","COM_SALDO","CONSULTAR","CONTRATO_DIGITADO","DESCONHECIDO","DISPARO","FINALIZADO","FORA_MODALIDADE","INSTABILIDADE","NAO_AUTORIZADO","CPF_INVALIDO","LEAD_NOVO","MUDANCAS_CADASTRAIS"]
            
            combinacoes = list(itertools.product(datas, etapa))

            # Cria o DataFrame com Qtd zerada
            df_clientes_atendidos_agrupados = pd.DataFrame(combinacoes, columns=['Data', 'Etapa'])
            df_clientes_atendidos_agrupados['Qtd'] = 0
            
        metric_card(f'Contatos realizados "{"todos" if selectbox_etapa == "Selecionar" else selectbox_etapa}"', f"{format(int(df_clientes_atendidos.shape[0]), ',').replace(',', '.')}")
        
        df_clientes_atendidos_agrupados = df_clientes_atendidos_agrupados.rename(columns={'etapa_padronizada': 'Etapa'})


    with col_4a:
        ##### CARD % DE ATENDIMENTOS DO TOTAL #####
        valor = f"{(df_clientes_atendidos_card.shape[0] / int(df_total_clientes_unicos['TOTAL_CPF']) * 100):.2f}".replace('.',',')
        metric_card("% de atendimentos do Total", f"{valor} %")

    ##### ÁREA DA TABELA #####
with st.container():
    col_1b, col_2b = st.columns((5,5))

    with col_1b:
        st.markdown("### :blue[Etapa por Data]")
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

    with col_2b:
        ##### TABELA DE CLIENTES #####
        st.markdown("### :blue[Detalhamento dos Clientes]")
        st.dataframe(df_clientes_atendidos, width='stretch', height=500, hide_index=True)

        ##### BOTÃO EXPORTAR TABELA #####
        csv = df_clientes_atendidos.to_csv(index=False)
        st.download_button(
            label="⬇️ Baixar planilha",
            data=csv,
            file_name="dados.csv",
            mime="text/csv",
        )
