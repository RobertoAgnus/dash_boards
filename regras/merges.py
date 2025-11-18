import pandas as pd
import numpy as np

def merge_crm_telefones_corban(df1, df2):
    ########## DF0 = CRM <- TELEFONES CORBAN ##########
    parte1 = pd.merge(
                    df1, 
                    df2[(df2['cpf_telefone_corban'].notna()) | 
                                        (df2['cpf_telefone_corban'].notnull())], 
                    left_on='cpf', 
                    right_on='cpf_telefone_corban', 
                    how='outer'
                )
    parte2 = pd.merge(
                    df1, 
                    df2[(df2['cpf_telefone_corban'].isna()) | 
                                        (df2['cpf_telefone_corban'].isnull())], 
                    left_on='telefone_aux1', 
                    right_on='telefone_corban', 
                    how='outer'
                )
    
    df0 = pd.concat([parte1, parte2], ignore_index=True)

    df0['cpf_aux0'] = df0['cpf'].replace('', np.nan).fillna(df0['cpf_telefone_corban'])
    df0['telefone_aux0'] = df0['telefone_aux1'].replace('', np.nan).fillna(df0['telefone_corban'])
    
    df1 = df0[['cpf_aux0', 'nome', 'telefone_aux0', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela']]

    df1 = df1.rename(columns={'cpf_aux0': 'cpf', 'telefone_aux0': 'telefone_aux1'})

    return df1

def merge_crm_disparos(df1, df2):
    ########## DF0 = CRM <- DISPAROS ##########
    parte1 = pd.merge(
                    df1, 
                    df2[(df2['cpf_disparos'].notna()) | 
                                (df2['cpf_disparos'].notnull())], 
                    left_on='cpf', 
                    right_on='cpf_disparos', 
                    how='outer'
                )
    parte2 = pd.merge(
                    df1, 
                    df2[(df2['cpf_disparos'].isna()) | 
                                (df2['cpf_disparos'].isnull())], 
                    left_on='telefone_aux1', 
                    right_on='telefone_disparos', 
                    how='outer'
                )
    
    df0 = pd.concat([parte1, parte2], ignore_index=True)

    df0['cpf_aux0'] = df0['cpf'].replace('', np.nan).fillna(df0['cpf_disparos'])
    df0['telefone_aux0'] = df0['telefone_aux1'].replace('', np.nan).fillna(df0['telefone_disparos'])
    
    df1 = df0[['cpf_aux0', 'nome', 'telefone_aux0', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela']]

    df1 = df1.rename(columns={'cpf_aux0': 'cpf', 'telefone_aux0': 'telefone_aux1'})

    return df1

def merge_crm_base_consolidada(df1, df2):
    ########## DF0 = CRM <- BASES CONSOLIDADAS ##########
    parte1 = pd.merge(
                    df1, 
                    df2[(df2['cpf_consolidado'].notna()) | 
                                        (df2['cpf_consolidado'].notnull())], 
                    left_on='cpf', 
                    right_on='cpf_consolidado', 
                    how='outer'
                )
    parte2 = pd.merge(
                    df1, 
                    df2[(df2['cpf_consolidado'].isna()) | 
                                        (df2['cpf_consolidado'].isnull())], 
                    left_on='telefone_aux1', 
                    right_on='telefone_consolidado', 
                    how='outer'
                )
    
    df0 = pd.concat([parte1, parte2], ignore_index=True)

    df0['cpf_aux0'] = df0['cpf'].replace('', np.nan).fillna(df0['cpf_consolidado'])
    df0['telefone_aux0'] = df0['telefone_aux1'].replace('', np.nan).fillna(df0['telefone_consolidado'])
    df0['nome_aux0'] = df0['nome'].replace('', np.nan).fillna(df0['nome_consolidado'])
    
    df1 = df0[['cpf_aux0', 'nome_aux0', 'telefone_aux0', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela']]

    df1 = df1.rename(columns={'cpf_aux0': 'cpf', 'nome_aux0': 'nome', 'telefone_aux0': 'telefone_aux1'})

    return df1

def merge_digisac_telefones_corban(df1, df2):
    ########## DF0 = DIGISAC <- TELEFONES CORBAN ##########
    parte1 = pd.merge(
                    df1, 
                    df2[(df2['cpf_telefone_corban'].notna()) | 
                                        (df2['cpf_telefone_corban'].notnull())], 
                    left_on='cpf_digisac', 
                    right_on='cpf_telefone_corban', 
                    how='outer'
                )
    parte2 = pd.merge(
                    df1, 
                    df2[(df2['cpf_telefone_corban'].isna()) | 
                                        (df2['cpf_telefone_corban'].isnull())], 
                    left_on='telefone_digisac', 
                    right_on='telefone_corban', 
                    how='outer'
                )
    
    df0 = pd.concat([parte1, parte2], ignore_index=True)

    df0['cpf_aux0'] = df0['cpf_digisac'].replace('', np.nan).fillna(df0['cpf_telefone_corban'])
    df0['telefone_aux0'] = df0['telefone_digisac'].replace('', np.nan).fillna(df0['telefone_corban'])
    
    cols = df0.columns.drop('cpf_aux0')

    df0 = df0.sort_values(['cpf_aux0', 'data_digisac'], ascending=[True, False])

    df_result = (
        df0.groupby('cpf_aux0', as_index=False)
        .agg({col: 'first' for col in cols})
    )

    df1 = df_result[['cpf_aux0', 'nome_interno', 'telefone_aux0', 'falha', 'data_digisac']]

    df1 = df1.rename(columns={'cpf_aux0': 'cpf_digisac', 'telefone_aux0': 'telefone_digisac'})

    return df1

def merge_digisac_disparos(df1, df2):
    ########## DF0 = DIGISAC <- DISPAROS ##########
    parte1 = pd.merge(
                    df1, 
                    df2[(df2['cpf_disparos'].notna()) | 
                                (df2['cpf_disparos'].notnull())], 
                    left_on='cpf_digisac', 
                    right_on='cpf_disparos', 
                    how='outer'
                )
    parte2 = pd.merge(
                    df1, 
                    df2[(df2['cpf_disparos'].isna()) | 
                                (df2['cpf_disparos'].isnull())], 
                    left_on='telefone_digisac', 
                    right_on='telefone_disparos', 
                    how='outer'
                )
    
    df0 = pd.concat([parte1, parte2], ignore_index=True)

    df0['cpf_aux0'] = df0['cpf_digisac'].replace('', np.nan).fillna(df0['cpf_disparos'])
    df0['telefone_aux0'] = df0['telefone_digisac'].replace('', np.nan).fillna(df0['telefone_disparos'])
    
    df1 = df0[['cpf_aux0', 'nome_interno', 'telefone_aux0', 'falha', 'data_digisac']]

    df1 = df1.rename(columns={'cpf_aux0': 'cpf_digisac', 'telefone_aux0': 'telefone_digisac'})

    return df1

def merge_digisac_base_consolidada(df1, df2):
    ########## DF0 = DIGISAC <- BASES CONSOLIDADAS ##########
    parte1 = pd.merge(
                    df1, 
                    df2[(df2['cpf_consolidado'].notna()) | 
                                        (df2['cpf_consolidado'].notnull())], 
                    left_on='cpf_digisac', 
                    right_on='cpf_consolidado', 
                    how='outer'
                )
    parte2 = pd.merge(
                    df1, 
                    df2[(df2['cpf_consolidado'].isna()) | 
                                        (df2['cpf_consolidado'].isnull())], 
                    left_on='telefone_digisac', 
                    right_on='telefone_consolidado', 
                    how='outer'
                )
    
    df0 = pd.concat([parte1, parte2], ignore_index=True)

    df0['cpf_aux0'] = df0['cpf_digisac'].replace('', np.nan).fillna(df0['cpf_consolidado'])
    df0['telefone_aux0'] = df0['telefone_digisac'].replace('', np.nan).fillna(df0['telefone_consolidado'])
    df0['nome_aux0'] = df0['nome_interno'].replace('', np.nan).fillna(df0['nome_consolidado'])
    
    cols = df0.columns.drop('cpf_aux0')

    df0 = df0.sort_values(['cpf_aux0', 'data_digisac'], ascending=[True, False])

    df_result = (
        df0.groupby('cpf_aux0', as_index=False)
        .agg({col: 'first' for col in cols})
    )

    df1 = df_result[['cpf_aux0', 'nome_aux0', 'telefone_aux0', 'falha', 'data_digisac']]

    df1 = df1.rename(columns={'cpf_aux0': 'cpf_digisac', 'nome_aux0': 'nome_interno', 'telefone_aux0': 'telefone_digisac'})

    return df1

def merge_crm_digisac(df1, df2):
    ########## DF1 = CRM <- DIGISAC ##########
    parte1 = pd.merge(
                    df1, 
                    df2[(df2['cpf_digisac'].notna()) | 
                               (df2['cpf_digisac'].notnull())], 
                    left_on='cpf', 
                    right_on='cpf_digisac', 
                    how='outer'
                )
    parte2 = pd.merge(
                    df1, 
                    df2[(df2['cpf_digisac'].isna()) | 
                               (df2['cpf_digisac'].isnull())], 
                    left_on='telefone_aux1', 
                    right_on='telefone_digisac', 
                    how='outer'
                )
    
    df = pd.concat([parte1, parte2], ignore_index=True)

    df['cpf_aux1'] = df['cpf'].replace('', np.nan).fillna(df['cpf_digisac'])
    df['nome_aux1'] = df['nome'].replace('', np.nan).fillna(df['nome_interno'])
    df['telefone_aux2'] = df['telefone_aux1'].replace('', np.nan).fillna(df['telefone_digisac'])
    
    cols = df.columns.drop('cpf_aux1')

    df = df.sort_values(['cpf_aux1', 'data_digisac'], ascending=[True, False])

    df_result = (
        df.groupby('cpf_aux1', as_index=False)
        .agg({col: 'first' for col in cols})
    )

    df = df_result[['cpf_aux1', 'nome_aux1', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'telefone_aux2', 'falha', 'data_digisac']]

    return df

def merge_df1_corban(df1, df2):
    ########## DF2 = DF1 <- CORBAN ##########
    parte1 = pd.merge(
                    df1, 
                    df2[(df2['cpf_corban'].notna()) | 
                              (df2['cpf_corban'].notnull())], 
                    left_on='cpf_aux1', 
                    right_on='cpf_corban', 
                    how='outer'
                )
    parte2 = pd.merge(
                    df1, 
                    df2[(df2['cpf_corban'].isna()) | 
                              (df2['cpf_corban'].isnull())], 
                    left_on='telefone_aux2', 
                    right_on='telefone_propostas', 
                    how='outer'
                )
    
    df = pd.concat([parte1, parte2], ignore_index=True)

    df['cpf_aux2'] = df['cpf_aux1'].replace('', np.nan).fillna(df['cpf_corban'])
    df['nome_aux2'] = df['nome_aux1'].replace('', np.nan).fillna(df['nome_corban'])
    df['telefone_aux3'] = df['telefone_aux2'].replace('', np.nan).fillna(df['telefone_propostas'])

    df = df.sort_values(['cpf_aux2', 'data_atualizacao_api'], ascending=[True, False])

    df = df[['cpf_aux2', 'nome_aux2', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'telefone_aux3', 'falha', 'data_digisac', 'status_api', 'data_atualizacao_api']]

    return df

def merge_df2_telefones_corban(df1, df2):
    ########## df = DF2 <- TELEFONES CORBAN ##########
    df = pd.merge(df1, df2, left_on=['cpf_aux2'], right_on=['cpf_telefone_corban'], how='outer')
    
    df['cpf_aux3'] = df['cpf_aux2'].replace('', np.nan).fillna(df['cpf_telefone_corban'])
    df['telefone_aux4'] = df['telefone_aux3'].replace('', np.nan).fillna(df['telefone_corban'])

    df = df.drop_duplicates(subset=['cpf_aux3', 'dataConsulta', 'telefone_aux4'])

    df = df[['cpf_aux3', 'nome_aux2', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'telefone_aux4', 'falha', 'data_digisac', 'status_api', 'data_atualizacao_api']]

    return df

def merge_df3_disparos(df1, df2):
    ########## df = DF3 <- DISPAROS ##########
    df = pd.merge(df1, df2, left_on=['cpf_aux3'], right_on=['cpf_disparos'], how='outer')
    
    df['cpf_aux4'] = df['cpf_aux3'].replace('', np.nan).fillna(df['cpf_disparos'])
    df['telefone_aux5'] = df['telefone_aux4'].replace('', np.nan).fillna(df['telefone_disparos'])

    df = df.drop_duplicates(subset=['cpf_aux4', 'dataConsulta', 'telefone_aux5'])

    cols = df.columns.drop('telefone_aux5')

    df = df.sort_values(['telefone_aux5', 'dataConsulta'], ascending=[True, False])

    df_result = (
        df.groupby('telefone_aux5', as_index=False)
        .agg({col: 'first' for col in cols})
    )

    df = df_result[['cpf_aux4', 'nome_aux2', 'dataConsulta', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'telefone_aux5', 'falha', 'data_digisac', 'status_api', 'data_atualizacao_api']]

    return df

def merge_df4_base_consolidada(df1, df2):
    ########## DF = DF4 <- BASES CONSOLIDADAS ##########
    df = pd.merge(df1, df2, left_on=['cpf_aux4'], right_on=['cpf_consolidado'], how='outer')
    
    df['cpf_aux5'] = df['cpf_aux4'].replace('', np.nan).fillna(df['cpf_consolidado'])
    df['nome_aux3'] = df['nome_aux2'].replace('', np.nan).fillna(df['nome_consolidado'])
    df['telefone_aux6'] = df['telefone_aux5'].replace('', np.nan).fillna(df['telefone_consolidado'])

    df = df[['dataConsulta', 'cpf_aux5', 'nome_aux3', 'telefone_aux6', 'erros', 'valorLiberado', 'valorContrato', 'parcelas', 'tabela', 'data_digisac', 'falha', 'data_atualizacao_api', 'status_api']]

    return df