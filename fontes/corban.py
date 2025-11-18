import pandas as pd

from querys.querys_sql import QuerysSQL
from regras.formatadores import formatar_cpf, formatar_telefone


def get_corban(conn):
    ##### CRIAR INSTÂNCIA ÚNICA #####
    consulta = QuerysSQL()

    consulta_corban = consulta.get_corban()
    df = pd.read_sql_query(consulta_corban, conn)

    df = formatar_cpf(df, 'cpf_corban')

    df = formatar_telefone(df, 'telefone_propostas')

    df['data_atualizacao_api'] = pd.to_datetime(df['data_atualizacao_api'])
    
    df = df.sort_values('data_atualizacao_api', ascending=False).drop_duplicates(subset=['cpf_corban','telefone_propostas'], keep='first')
    
    df['data_atualizacao_api'] = df['data_atualizacao_api'].dt.date

    df = df[['cpf_corban', 'nome_corban', 'telefone_propostas', 'status_api', 'data_atualizacao_api']]

    return df