from datetime import datetime
import pandas as pd
import duckdb as dk
import csv

path_1 = 'C:/workspace/scripts_python/dash_boards/arquivos'
path_2 = "C:/workspace/scripts_python/arquivos/CSV/corban"

clientes         = pd.read_csv(
                        f'{path_1}/clientes.csv', 
                        sep=',', 
                        encoding='UTF-8', 
                        quoting=csv.QUOTE_ALL, 
                        quotechar='"', 
                        on_bad_lines='skip', 
                        engine='python')
atendimentos     = pd.read_csv(
                        f'{path_1}/atendimentos.csv', 
                        sep=',', 
                        encoding='UTF-8', 
                        quoting=csv.QUOTE_ALL, 
                        quotechar='"', 
                        on_bad_lines='skip', 
                        engine='python')
telefones        = pd.read_csv(
                        f'{path_1}/telefones.csv', 
                        sep=',', 
                        encoding='UTF-8', 
                        quoting=csv.QUOTE_ALL, 
                        quotechar='"', 
                        on_bad_lines='skip', 
                        engine='python'
                    )
contratos        = pd.read_csv(
                        f'{path_1}/contratos.csv', sep=',', 
                        encoding='UTF-8', 
                        quoting=csv.QUOTE_ALL, 
                        quotechar='"', 
                        on_bad_lines='skip', 
                        engine='python'
                    )
contratos_corban = pd.read_csv(
                        f'{path_1}/contratos_corban.csv', 
                        sep=',', 
                        encoding='UTF-8', 
                        quoting=csv.QUOTE_ALL, 
                        quotechar='"', 
                        on_bad_lines='skip', 
                        engine='python'
                    )

propostas        = pd.read_csv(
                        f'{path_2}/propostas/propostas_concatenado.csv', 
                        low_memory=False
                    )
comissoes        = pd.read_csv(
                        f'{path_2}/comissoes/comissoes_concatenado.csv', 
                        low_memory=False
                    )
comissionamento  = pd.read_csv(
                        f'{path_2}/comissionamentos/comissionamentos_concatenado.csv', 
                        low_memory=False
                    )

dk.register("clientes", clientes)
dk.register("atendimentos", atendimentos)
dk.register("telefones", telefones)
dk.register("contratos", contratos)
dk.register("contratos_corban", contratos_corban)

dk.register("propostas", propostas)
dk.register("comissoes", comissoes)
dk.register("comissionamento", comissionamento)

# Cria conexão com DuckDB
con = dk.connect()

# Registra o DataFrame como uma tabela no contexto do DuckDB
con.register("atendimentos", atendimentos)

class QuerysSQL:
    def __init__(self):
        ...

    def clientes_atendidos(self, status=None, data=None):
        condicao = None
        if status == 'Selecionar' and data != 'Selecionar':
            condicao = f"""where a.data {data}"""
        elif data == 'Selecionar' and status != 'Selecionar':
            condicao = f"""where a.status='{status}'"""
        elif data != 'Selecionar' and status != 'Selecionar':
            condicao = f"""where a.status='{status}'
                            and a.data {data}"""
        elif status == 'Selecionar' and data == 'Selecionar':
            condicao = "where 1 = 1"

        query = f"""select 
                        strftime(CAST(a.data AS DATE), '%d/%m/%Y') as Data,
                        c.CPF as CPF, 
                        c.nome as Nome, 
                        t.telefone as Telefone, 
                        c.cidade as Cidade, 
                        c.estado as UF, 
                        a.chatId as "Chat ID", 
                        a.etapa as Etapa, 
                        a.status as Status
                    from clientes c 
                    inner join atendimentos a 
                    on c.id = a.clienteId
                    inner join telefones t
                    on c.id = t.clienteId
                    {condicao}
                    order by a.data desc;"""
        
        return query
    
    def total_clientes(self):
        query = f"""select distinct 
                        c.CPF as TOTAL_CPF
                    from clientes c 
                    inner join atendimentos a 
                    on c.id = a.clienteId
                    inner join telefones t
                    on c.id = t.clienteId;"""
        
        return query
    
    def status_atendimentos(self):
        query = '''select 
                        a.status as Status
                    from atendimentos a;'''
        
        return query
    
    def disparos_por_cliente(self, status=None, data=None, disparos=None):
        condicao = None
        if status == 'Selecionar' and data != 'Selecionar':
            condicao = f"""where a.data {data}"""
        elif data == 'Selecionar' and status != 'Selecionar':
            condicao = f"""where a.status='{status}'"""
        elif data != 'Selecionar' and status != 'Selecionar':
            condicao = f"""where a.status='{status}'
                            and a.data {data}"""
        elif status == 'Selecionar' and data == 'Selecionar':
            condicao = "where 1 = 1"

        query = f"""select 
                    strftime(CAST(a.data AS DATE), '%d/%m/%Y') as Data,
                    c.CPF as CPF, 
                    c.nome as Nome, 
                    t.telefone as Telefone, 
                    c.cidade as Cidade, 
                    c.estado as UF, 
                    a.chatId as CHAT_ID, 
                    a.etapa as Etapa, 
                    a.status as Status
                from clientes c 
                inner join atendimentos a 
                on c.id = a.clienteId
                inner join telefones t
                on c.id = t.clienteId 
                {condicao}
                order by a.data desc;"""
        
        return query
    
    def contagem_de_disparos(self, status=None, data=None):
        condicao = None
        if status == 'Selecionar' and data != 'Selecionar':
            condicao = f"""where a.data {data}"""
        elif data == 'Selecionar' and status != 'Selecionar':
            condicao = f"""where a.status='{status}'"""
        elif data != 'Selecionar' and status != 'Selecionar':
            condicao = f"""where a.status='{status}'
                            and a.data {data}"""
        elif status == 'Selecionar' and data == 'Selecionar':
            condicao = "where 1 = 1"

        query = f"""with consulta as (
                        select 
                            t.telefone as Telefone, 
                            strftime(CAST(a.data AS DATE), '%d/%m/%Y') as Data
                        from clientes c 
                        inner join atendimentos a 
                        on c.id = a.clienteId
                        inner join telefones t
                        on c.id = t.clienteId 
                        {condicao}
                    )
                    SELECT
                        COUNT(*) AS TOTAL
                    FROM
                        consulta
                    GROUP BY
                        Data,
                        Telefone;"""
        return query
    
    def clientes_novos(self, tipo):
        if tipo == 'Todos':
            condicao = 'where 1 = 1'
        elif tipo == 'Sem Contrato':
            condicao = 'where ct.prazo is null and ctb.prazo is null'
        elif tipo == 'Com Contrato':
            condicao = 'where ct.prazo is not null or ctb.prazo is not null'

        query = f"""select distinct
                        ct.dataInclusao as "Inclusão",
                        ctb.inclusao as "Inclusão Corban",
                        c.CPF as CPF, 
                        c.nome as Nome,
                        t.telefone as Telefone
                    from clientes c
                    inner join telefones t
                    on c.id = t.clienteId
                    left join contratos ct
                    on c.id = ct.clienteId
                    left join contratos_corban ctb
                    on c.id = ctb.clienteId
                    {condicao};"""
        
        return query
    
    def qtd_clientes_novos(self):
        query = f"""select distinct
                        c.CPF as CPF
                    from clientes c
                    inner join telefones t
                    on c.id = t.clienteId
                    left join contratos ct
                    on c.id = ct.clienteId
                    left join contratos_corban ctb
                    on c.id = ctb.clienteId
                    where ct.prazo is null
                    and ctb.prazo is null;"""
        return query
    
    def qtd_clientes(self):
        query = f"""select distinct
                        c.CPF as CPF
                    from clientes c;"""
        return query
    
    def data_proposta_corban(self):
        query = f"""select
                        p.data_status as data
                    from propostas p;"""
        return query
    
    def origem_proposta_corban(self):
        query = f"""select distinct
                        p.origem as origem
                    from propostas p
                    order by p.origem;"""
        return query

    def join_corban(self, origem, data):
        condicao = None
        if origem == 'Selecionar':
            condicao = f"""where p.data_status {data}"""
        elif origem != 'Selecionar':
            condicao = f"""where p.origem='{origem}'
                            and p.data_status {data}"""
        
        query = f"""select
                        p.data_status as 'Data Status', 
                        p.origem as Origem, 
                        p.status_nome as Status, 
                        c.valor as Valor, 
                        c.data as 'Data da Comissão', 
                        co.percentual_valor_base as 'Percetual Valor Base', 
                        co.valor_fixo as 'Valor Fixo'
                    from propostas p
                    left join comissoes c
                    on p.proposta_id = c.proposta_id
                    left join comissionamento co
                    on p.proposta_id = co.proposta_id
                    {condicao}
                    order by p.data_status;"""
        return query