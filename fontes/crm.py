import pandas as pd
import numpy as np

from querys.querys_sql import QuerysSQL
from regras.formatadores import formatar_cpf, formatar_telefone


def get_crm(conn):
    ##### CRIAR INSTÂNCIA ÚNICA #####
    consulta = QuerysSQL()

    ## Consulta CRM_Consulta
    consulta_crm_consulta = consulta.get_crm_consulta()
    df_consulta1 = pd.read_sql(consulta_crm_consulta, conn)

    df_consulta1['dataConsulta'] = pd.to_datetime(df_consulta1['dataConsulta'])
    
    df_consulta1 = formatar_cpf(df_consulta1, 'cpf')

    pega_clienteId = df_consulta1.groupby('cpf')['clienteId'].first().reset_index()
    
    df_consulta = pd.merge(df_consulta1, pega_clienteId, on='cpf', how='left')
    
    df_consulta = df_consulta.loc[
        df_consulta.groupby('cpf')['dataConsulta'].idxmax()
    ]
    
    ## Consulta CRM_Tabela
    consulta_crm_tabela = consulta.get_crm_tabela()
    df_tabela = pd.read_sql(consulta_crm_tabela, conn)
    
    ## Consulta CRM_Parcela
    consulta_crm_parcela = consulta.get_crm_parcela()
    df_parcela = pd.read_sql(consulta_crm_parcela, conn)

    ## Ajuste de tipos e realiza os JOIN's
    df_consulta['tabelaId'] = df_consulta['tabelaId'].astype('Int64')
    df_tabela['tabelaId'] = df_tabela['tabelaId'].astype(int)

    df1 = pd.merge(df_consulta, df_parcela, on='consultaId', how='left')
    
    df2 = pd.merge(df1, df_tabela, on='tabelaId', how='left')
    
    ## Consulta CRM_Cliente
    consulta_crm_cliente = consulta.get_crm_cliente()
    df_cliente = pd.read_sql(consulta_crm_cliente, conn)
    
    df_cliente = formatar_cpf(df_cliente, 'cpf')
    
    ## Consulta CRM_Telefone
    consulta_crm_telefone = consulta.get_crm_telefone()
    df_telefone = pd.read_sql(consulta_crm_telefone, conn)

    df_telefone = formatar_telefone(df_telefone, 'telefone_crm')

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
    
    ## Consulta CRM_Lead
    consulta_crm_lead = consulta.get_crm_lead()
    df_lead = pd.read_sql(consulta_crm_lead, conn)

    df_lead = formatar_telefone(df_lead, 'telefone_lead')

    #######################################################################################
    
    df3 = pd.merge(df2, df_lead, on='consultaId', how='outer')
    df3['nome'] = None
    df3['telefone_crm'] = None
    df3['erros'] = df3['erros'].fillna('Sucesso')
    df3 = df3[['cpf', 'clienteId', 'telefone_lead', 'nome', 'telefone_crm', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'banco']]
    
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

    df['dataConsulta'] = df['dataConsulta'].dt.date

    df['telefone_aux1'] = df['telefone_crm'].replace('', np.nan).fillna(df['telefone_lead'])
    
    df = df[['cpf', 'nome', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'banco', 'telefone_aux1']]
    
    df = df.drop_duplicates(subset=['cpf','telefone_aux1'])

    return df