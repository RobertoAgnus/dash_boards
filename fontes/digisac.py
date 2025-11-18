import pandas as pd

from querys.querys_sql import QuerysSQL
from regras.formatadores import formatar_cpf, formatar_telefone


def get_digisac(conn):
    ##### CRIAR INSTÂNCIA ÚNICA #####
    consulta = QuerysSQL()

    consulta_digisac = consulta.get_digisac()
    df = pd.read_sql_query(consulta_digisac, conn)
    df['data_digisac'] = pd.to_datetime(df['data'])

    df = formatar_telefone(df, 'telefone_digisac')

    df = formatar_cpf(df, 'cpf_digisac')
    
    df.loc[df['nome_interno'].str.contains('CPF', case=False, na=False), 'cpf_digisac'] = df['nome_interno'].str.split('CPF:').str[1].str.strip()

    # df['prioridade'] = (
    #     df['cpf_digisac'].notna().astype(int)
    #     + df['nome_interno'].notna().astype(int)
    # )

    # df = df.sort_values(
    #     by=['data_digisac', 'telefone_digisac', 'prioridade'],
    #     ascending=[True, True, False]
    # )

    # df = df.drop_duplicates(
    #     subset=['data_digisac', 'telefone_digisac'],
    #     keep='first'
    # )

    df['data_digisac'] = df['data_digisac'].dt.date

    df = df[['cpf_digisac', 'nome_interno', 'telefone_digisac', 'falha', 'data_digisac']]

    return df