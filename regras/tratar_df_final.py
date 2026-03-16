

def trata_df_final(df):
    # Condição: data_inicio <= data_fim
    cond = df['dataConsulta'] <= df['data_atualizacao_api']

    # Aplicando a regra
    df.loc[~cond, ['status_api', 'data_atualizacao_api']] = None

    renomear = {
                    'dataConsulta': 'Data Consulta',
                    'cpf_aux2': 'CPF',
                    'nome_aux2': 'Nome',
                    'telefone_aux3': 'Telefone',
                    'erros': 'Retorno Consulta',
                    'tabela': 'Tabelas',
                    'banco': 'Banco',
                    'data_digisac': 'Data Disparo',
                    'parcelas': 'Parcelas',
                    'valorLiberado': 'Valor Liberado',
                    'valorContrato': 'Valor Contrato',
                    'falha': 'Retorno Digisac',
                    'data_atualizacao_api': 'Data Corban',
                    'status_api': 'Status Corban'
                }

    df = df.rename(columns=renomear)

    df['Telefone'] = df['Telefone'].replace('nan', None)

    df.loc[df['Nome'].str.contains('CPF', case=False, na=False), 'CPF'] = df['Nome'].str.split('CPF:').str[1].str.strip()
    df.loc[df['CPF'].str.contains('Sem CPF', case=False, na=False), 'CPF'] = None
    df.loc[df['Nome'].str.contains('Sem nome', case=False, na=False), 'Nome'] = None

    return df.drop_duplicates(subset=['CPF', 'Telefone', 'Data Consulta', 'Data Disparo', 'Data Corban'])