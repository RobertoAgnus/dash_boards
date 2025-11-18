import pandas as pd

from querys.querys_sql import QuerysSQL
from regras.formatadores import formatar_cpf, formatar_telefone


def get_telefones_corban(conn):
    ##### CRIAR INSTÂNCIA ÚNICA #####
    consulta = QuerysSQL()

    consulta_telefone_corban = consulta.get_telefones_corban()
    df = pd.read_sql_query(consulta_telefone_corban, conn)

    df = formatar_cpf(df, 'cpf_telefone_corban')

    df = formatar_telefone(df, 'telefone_corban')

    df = df.drop_duplicates(subset=['cpf_telefone_corban','telefone_corban'])

    return df
