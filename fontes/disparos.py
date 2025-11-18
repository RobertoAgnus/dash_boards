import pandas as pd

from querys.querys_sql import QuerysSQL
from regras.formatadores import formatar_cpf, formatar_telefone


def get_disparos(conn):
    ##### CRIAR INSTÂNCIA ÚNICA #####
    consulta = QuerysSQL()

    consulta_disparos = consulta.disparos()
    df = pd.read_sql_query(consulta_disparos, conn)

    df = formatar_cpf(df, 'cpf_disparos')

    df = formatar_telefone(df, 'telefone_disparos')

    df['telefone_disparos'] = df['telefone_disparos'].astype(str).apply(
        lambda x: x[:2] + x[3:] if len(x) > 11 and x.isdigit() else x
    )

    df = df.drop_duplicates(subset=['cpf_disparos', 'telefone_disparos'])

    return df