import pandas as pd
import streamlit as st

from querys.connect import Conexao
from querys.querys_sql import QuerysSQL
from fontes import telefones_corban, disparos, base_consolidada, crm, digisac, corban
from regras.tratar_df_final import trata_df_final
from regras.merges import merge_crm_telefones_corban, merge_crm_disparos, merge_crm_base_consolidada, merge_digisac_telefones_corban, merge_digisac_disparos, merge_digisac_base_consolidada, merge_crm_digisac, merge_df1_corban, merge_df2_telefones_corban, merge_df3_disparos, merge_df4_base_consolidada


##### CACHE DE CONSULTAS #####
@st.cache_data(show_spinner=False)
def carregar_dados(pagina):
    conectar = Conexao()
    consulta = QuerysSQL()

    # conectar.conectar_mysql_aws()
    conectar.conectar_postgres_aws()
    conectar.conectar_postgres()

    # conn_mysql_aws    = conectar.obter_conexao_mysql_aws()
    conn_postgres_aws = conectar.obter_conexao_postgres_aws()
    conn_postgres     = conectar.obter_conexao_postgres()
    
    if pagina == 'clientes_atendidos':
        # == CRM ==========================
        df_crm = crm.get_crm(conn_postgres_aws, conn_postgres_aws)
        
        # == DIGISAC ======================
        df_digisac = digisac.get_digisac(conn_postgres)
        
        # == CORBAN =======================
        df_corban = corban.get_corban(conn_postgres)
        
        # == DF1 = CRM <- DIGISAC =========
        df1 = merge_crm_digisac(df_crm, df_digisac)
        
        # == DF2 = DF1 <- CORBAN ==========
        df = merge_df1_corban(df1, df_corban)
        
        
        df = trata_df_final(df)

        return df, df_crm, df_digisac, df_corban
    
    elif pagina == 'acompanhamento':
        digisac, corban, crm = consulta.get_acompanhamento()

        df_digisac = pd.read_sql_query(digisac, conn_postgres)
        df_corban  = pd.read_sql_query(corban, conn_postgres)
        df_crm     = pd.read_sql_query(crm, conn_postgres_aws)

        # ============= TRATAMENTOS =============
        df_01 = pd.merge(df_digisac, df_corban, on='telefone', how='left')
        df = pd.merge(df_01, df_crm, on='telefone', how='left')

        return df
    
    elif pagina == 'campanhas':
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

        df_corban = pd.merge(df_corban, df_fones_crm, on='cpf_corban', how='left')
        df_corban = pd.merge(df_corban, df_comissoes, on='proposta_id', how='left')

        sem_fones = df_corban[df_corban['numero_corban'].isnull()]

        cpfs_sem_fone = sem_fones['cpf_corban'].unique().tolist()

        df_fones_corban = pd.read_sql_query(fones_corban, conn_postgres, params=(cpfs_sem_fone,))

        df_corban = pd.merge(df_corban, df_fones_corban, on='cpf_corban', how='left')

        return df_corban, df_crm, custo_campanhas, df_fones_crm, df_comissoes, df_tabelas
    
    elif pagina == 'disparos_digisac':
        clientes = consulta.get_clientes_digisac()
        tickets  = consulta.get_tickets_gigisac()
        tags     = consulta.get_tags_digisac()
        disparos = consulta.get_disparados_digisac()
        falhas   = consulta.get_falhas_digisac()
        corban   = consulta.get_corban()
        crm      = consulta.get_crm()

        df_clientes = pd.read_sql_query(clientes, conn_postgres    )
        df_tickets  = pd.read_sql_query(tickets , conn_postgres    )
        df_tags     = pd.read_sql_query(tags    , conn_postgres    )
        df_disparos = pd.read_sql_query(disparos, conn_postgres    )
        df_falhas   = pd.read_sql_query(falhas  , conn_postgres    )
        df_corban   = pd.read_sql_query(corban  , conn_postgres    )
        df_crm      = pd.read_sql_query(crm     , conn_postgres_aws)

        df_01 = pd.merge(df_clientes, df_tickets , on='number'   , how='left')
        df_02 = pd.merge(df_01      , df_tags    , on='ticket_id', how='left')
        df_03 = pd.merge(df_02      , df_disparos, on='number'   , how='left')
        df_04 = pd.merge(df_03      , df_falhas  , on='number'   , how='left')
        df_05 = pd.merge(df_04      , df_corban  , on='number'   , how='left')
        df    = pd.merge(df_05      , df_crm     , on='number'   , how='left')

        df = df[(df['pagamento_corban'].isna()) & (df['pagamento_crm'].isna())]

        return df