import pandas as pd

from querys.querys_sql import QuerysSQL
from regras.formatadores import formatar_cpf, formatar_telefone


def get_base_consolidada(conn):
    ##### CRIAR INSTÂNCIA ÚNICA #####
    consulta = QuerysSQL()

    consulta_base_consolidada = consulta.get_base_consolidada()
    df = pd.read_sql_query(consulta_base_consolidada, conn)

    df = formatar_cpf(df, 'cpf_consolidado')

    df = formatar_telefone(df, 'telefone_consolidado')

    df['telefone_consolidado'] = df['telefone_consolidado'].astype(str).apply(
        lambda x: x[:2] + x[3:] if len(x) > 11 and x.isdigit() else x
    )

    return df