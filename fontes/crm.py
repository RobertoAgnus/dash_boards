import pandas as pd
import numpy as np

from querys.querys_sql import QuerysSQL
from regras.formatadores import Regras


def get_crm(conn_postgres_aws, conn_mysql_aws):
    ##### CRIAR INSTÂNCIA ÚNICA #####
    consulta = QuerysSQL()
    regras = Regras()

    def obter_consulta(query, conn):
        df_consulta1 = pd.read_sql(query, conn)

        df_consulta1['dataConsulta'] = pd.to_datetime(df_consulta1['dataConsulta'])
        
        df_consulta1 = regras.formatar_cpf(df_consulta1, 'cpf')

        pega_clienteId = df_consulta1.groupby('cpf')['clienteId'].first().reset_index()
        
        df_consulta = pd.merge(df_consulta1, pega_clienteId, on='cpf', how='left')
        
        df_consulta = df_consulta.loc[
            df_consulta.groupby('cpf')['dataConsulta'].idxmax()
        ]

        return df_consulta
    

    # ## Consulta CRM_Consulta MySQL AWS
    # consulta_crm_consulta_mysql = consulta.get_crm_consulta_mysql_aws()
    # df_consulta_crm = obter_consulta(consulta_crm_consulta_mysql, conn_mysql_aws)

    ## Consulta CRM_Consulta PostgreSQL AWS
    consulta_crm_consulta_postgres = consulta.get_crm_consulta_postgres_aws()
    df_consulta_postgres = obter_consulta(consulta_crm_consulta_postgres, conn_postgres_aws)

    
    # df_consulta = pd.concat([df_consulta_crm, df_consulta_postgres], ignore_index=True)

    df_consulta = df_consulta_postgres.drop_duplicates()
    

    # ## Consulta CRM_Tabela MySQL AWS
    # consulta_crm_tabela_mysql_aws = consulta.get_crm_tabela_mysql_aws()
    # df_tabela_mysql_aws = pd.read_sql(consulta_crm_tabela_mysql_aws, conn_mysql_aws)

    ## Consulta CRM_Tabela PostgreSQL AWS
    consulta_crm_tabela_postgres_aws = consulta.get_crm_tabela_postgres_aws()
    df_tabela_postgres_aws = pd.read_sql(consulta_crm_tabela_postgres_aws, conn_postgres_aws)

    # df_tabela = pd.concat([df_tabela_mysql_aws, df_tabela_postgres_aws], ignore_index=True)

    df_tabela = df_tabela_postgres_aws.drop_duplicates()

    
    # ## Consulta CRM_Parcela MySQL AWS
    # consulta_crm_parcela_mysql_aws = consulta.get_crm_parcela_mysql_aws()
    # df_parcela_mysql_aws = pd.read_sql(consulta_crm_parcela_mysql_aws, conn_mysql_aws)

    ## Consulta CRM_Parcela PostgreSQL AWS
    consulta_crm_parcela_postgres_aws = consulta.get_crm_parcela_postgres_aws()
    df_parcela_postgres_aws = pd.read_sql(consulta_crm_parcela_postgres_aws, conn_postgres_aws)

    # df_parcela = pd.concat([df_parcela_mysql_aws, df_parcela_postgres_aws], ignore_index=True)

    df_parcela = df_parcela_postgres_aws.drop_duplicates()


    ## Ajuste de tipos e realiza os JOIN's
    df_consulta['tabelaId'] = (
        pd.to_numeric(df_consulta['tabelaId'], errors='coerce')
        .astype('Int64')
    )
    df_tabela['tabelaId']   = df_tabela['tabelaId'].astype(int)

    df1 = pd.merge(df_consulta, df_parcela, on='consultaId', how='left')
    
    df2 = pd.merge(df1, df_tabela, on='tabelaId', how='left')
    

    # ## Consulta CRM_Cliente MySQL AWS
    # consulta_crm_cliente_mysql_aws = consulta.get_crm_cliente_mysql_aws()
    # df_cliente_mysql_aws = pd.read_sql(consulta_crm_cliente_mysql_aws, conn_mysql_aws)

    ## Consulta CRM_Cliente PostgreSQL AWS
    consulta_crm_cliente_postgres_aws = consulta.get_crm_cliente_postgres_aws()
    df_cliente_postgres_aws = pd.read_sql(consulta_crm_cliente_postgres_aws, conn_postgres_aws)

    # df_cliente = pd.concat([df_cliente_mysql_aws, df_cliente_postgres_aws], ignore_index=True)

    df_cliente = regras.formatar_cpf(df_cliente_postgres_aws, 'cpf')

    df_cliente = df_cliente.drop_duplicates()

    
    # ## Consulta CRM_Telefone MySQL AWS
    # consulta_crm_telefone_mysql_aws = consulta.get_crm_telefone_mysql_aws()
    # df_telefone_mysql_aws = pd.read_sql(consulta_crm_telefone_mysql_aws, conn_mysql_aws)

    ## Consulta CRM_Telefone PostgreSQL AWS
    consulta_crm_telefone_postgres_aws = consulta.get_crm_telefone_postgres_aws()
    df_telefone_postgres_aws = pd.read_sql(consulta_crm_telefone_postgres_aws, conn_postgres_aws)

    # df_telefone = pd.concat([df_telefone_mysql_aws, df_telefone_postgres_aws], ignore_index=True)

    df_telefone = regras.formatar_telefone(df_telefone_postgres_aws, 'telefone_crm')

    df_telefone['telefone_crm'] = df_telefone['telefone_crm'].astype(str).str.replace(
            r'^(11)(?=\d{11,})', 
            '', 
            regex=True
        )
    
    df_telefone['telefone_crm'] = df_telefone['telefone_crm'].astype(str).str.replace(
            r'^(1)(?=\d{11,})', 
            '', 
            regex=True
        )
    
    df_telefone = df_telefone.drop_duplicates()

    
    # ## Consulta sistema_Lead MySQL AWS
    # consulta_sistema_lead_mysql_aws = consulta.get_sistema_lead_mysql_aws()
    # df_lead_sistema_mysql_aws = pd.read_sql(consulta_sistema_lead_mysql_aws, conn_mysql_aws)

    # ## Consulta CRM_Lead MySQL AWS
    # consulta_crm_lead_mysql_aws = consulta.get_crm_lead_mysql_aws()
    # df_lead_crm_mysql_aws = pd.read_sql(consulta_crm_lead_mysql_aws, conn_mysql_aws)

    ## Consulta CRM_AutoAtendimento PostgreSQL AWS
    consulta_crm_autoatendimento_postgres_aws = consulta.get_crm_autoatendimento_postgres_aws()
    df_autoatendimento_crm_postgres_aws = pd.read_sql(consulta_crm_autoatendimento_postgres_aws, conn_postgres_aws)

    # df_lead = pd.concat([df_lead_sistema_mysql_aws, df_lead_crm_mysql_aws, df_autoatendimento_crm_postgres_aws], ignore_index=True)

    df_lead = regras.formatar_telefone(df_autoatendimento_crm_postgres_aws, 'telefone_lead')

    df_lead = df_lead.drop_duplicates()


    #######################################################################################
    
    df3 = pd.merge(df2, df_lead, on='consultaId', how='outer')
    df3['nome'] = None
    df3['telefone_crm'] = None
    df3['erros'] = df3['erros'].fillna('Sucesso')
    df3 = df3[['cpf', 'clienteId', 'telefone_lead', 'nome', 'telefone_crm', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabelaId', 'banco']]
    
    df4 = pd.merge(df_cliente, df_telefone, on='clienteId', how='outer')
    
    df5 = pd.merge(df4, df_lead, on='clienteId', how='outer')
    df5['dataConsulta'] = None
    df5['erros'] = None
    df5['tabelaId'] = None
    df5['valorLiberado'] = None
    df5['valorContrato'] = None
    df5['parcelas'] = None
    df5['tabela'] = None
    df5['banco'] = None
    df5 = df5[['cpf', 'clienteId', 'telefone_lead', 'nome', 'telefone_crm', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'banco']]
    
    df  = pd.concat([df3, df5], ignore_index=True)

    df['dataConsulta'] = (
        pd.to_datetime(df['dataConsulta'], errors='coerce')
        .dt.date
    )

    df['telefone_aux1'] = df['telefone_crm'].replace('', np.nan).fillna(df['telefone_lead'])
    
    df = df[['cpf', 'nome', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'banco', 'telefone_aux1']]
    
    df = df.drop_duplicates(subset=['cpf','telefone_aux1'])

    return df