import streamlit as st

from querys.connect import Conexao
from fontes import telefones_corban, disparos, base_consolidada, crm, digisac, corban
from regras.tratar_df_final import trata_df_final
from regras.merges import merge_crm_telefones_corban, merge_crm_disparos, merge_crm_base_consolidada, merge_digisac_telefones_corban, merge_digisac_disparos, merge_digisac_base_consolidada, merge_crm_digisac, merge_df1_corban, merge_df2_telefones_corban, merge_df3_disparos, merge_df4_base_consolidada


##### CACHE DE CONSULTAS #####
@st.cache_data(show_spinner=False)
def carregar_dados():
    conectar = Conexao()

    conectar.conectar_postgres_aws()
    conectar.conectar_postgres()

    conn_postgres_aws = conectar.obter_conexao_postgres_aws()
    conn_postgres = conectar.obter_conexao_postgres()
    
    ########## TELEFONES CORBAN ##########
    df_telefones_corban = telefones_corban.get_telefones_corban(conn_postgres)
    ######################################
    
    ########## DISPAROS ##########
    df_disparos = disparos.get_disparos(conn_postgres)
    ##############################

    ########## BASES CONSOLIDADAS ##########
    df_base_consolidada = base_consolidada.get_base_consolidada(conn_postgres)
    ########################################
    
    ########## CRM ##########
    df_crm = crm.get_crm(conn_postgres_aws)
    #########################
    
    ########## DIGISAC ##########
    df_digisac = digisac.get_digisac(conn_postgres)
    #############################
    
    ########## CORBAN ##########
    df_corban = corban.get_corban(conn_postgres)
    ############################

    ########## DF0 = CRM <- TELEFONES CORBAN ##########
    df_crm = merge_crm_telefones_corban(df_crm, df_telefones_corban)
    ##########################################
    
    ########## DF0 = CRM <- DISPAROS ##########
    df_crm = merge_crm_disparos(df_crm, df_disparos)
    ##########################################

    ########## DF0 = CRM <- BASES CONSOLIDADAS ##########
    df_crm = merge_crm_base_consolidada(df_crm, df_base_consolidada)
    ##########################################

    # ########## DF0 = DIGISAC <- TELEFONES CORBAN ##########
    # df_digisac = merge_digisac_telefones_corban(df_digisac, df_telefones_corban)
    # ##########################################
    
    # ########## DF0 = DIGISAC <- DISPAROS ##########
    # df_digisac = merge_digisac_disparos(df_digisac, df_disparos)
    # ##########################################

    # ########## DF0 = DIGISAC <- BASES CONSOLIDADAS ##########
    # df_digisac = merge_digisac_base_consolidada(df_digisac, df_base_consolidada)
    # ##########################################

    ########## DF1 = CRM <- DIGISAC ##########
    df1 = merge_crm_digisac(df_crm, df_digisac)
    ##########################################
    
    ########## DF2 = DF1 <- CORBAN ##########
    df2 = merge_df1_corban(df1, df_corban)
    ########################################
    
    ########## DF3 = DF2 <- TELEFONES CORBAN ##########
    df3 = merge_df2_telefones_corban(df2, df_telefones_corban)
    ########################################
    
    ########## DF4 = DF3 <- DISPAROS ##########
    df4 = merge_df3_disparos(df3, df_disparos)
    ########################################
    
    ########## DF = DF4 <- BASES CONSOLIDADAS ##########
    df = merge_df4_base_consolidada(df4, df_base_consolidada)
    ########################################
    
    df = trata_df_final(df)

    return df, df_crm, df_digisac, df_corban
