import streamlit as st

from querys.connect import Conexao
from fontes import telefones_corban, disparos, base_consolidada, crm, digisac, corban
from regras.tratar_df_final import trata_df_final
from regras.merges import merge_crm_telefones_corban, merge_crm_disparos, merge_crm_base_consolidada, merge_digisac_telefones_corban, merge_digisac_disparos, merge_digisac_base_consolidada, merge_crm_digisac, merge_df1_corban, merge_df2_telefones_corban, merge_df3_disparos, merge_df4_base_consolidada


##### CACHE DE CONSULTAS #####
@st.cache_data(show_spinner=False)
def carregar_dados(pagina):
    conectar = Conexao()

    conectar.conectar_mysql_aws()
    conectar.conectar_postgres_aws()
    conectar.conectar_postgres()

    # conn_mysql_aws    = conectar.obter_conexao_mysql_aws()
    conn_postgres_aws = conectar.obter_conexao_postgres_aws()
    conn_postgres     = conectar.obter_conexao_postgres()
    
    if pagina == 'clientes_atendidos':
        ################################### CRM ###################################
        df_crm = crm.get_crm(conn_postgres_aws, conn_postgres_aws)
        
        ################################# DIGISAC #################################
        df_digisac = digisac.get_digisac(conn_postgres)
        
        ################################# CORBAN ##################################
        df_corban = corban.get_corban(conn_postgres)
        
        ########################## DF1 = CRM <- DIGISAC ###########################
        df1 = merge_crm_digisac(df_crm, df_digisac)
        
        ########################### DF2 = DF1 <- CORBAN ###########################
        df = merge_df1_corban(df1, df_corban)
        
        
        df = trata_df_final(df)

        return df, df_crm, df_digisac, df_corban
    elif pagina == 'acompanhamento':
        ...
