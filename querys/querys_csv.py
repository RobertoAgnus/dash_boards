import pandas as pd
import duckdb as dk
import csv
import streamlit as st

# Caminho dos arquivos localmente (será removido futuramente)
path_1 = st.secrets["paths"]["PATH_1"]
path_2 = st.secrets["paths"]["PATH_2"]

# ID's dos arquivos baixados do Banco de Dados (será feita conexão direta futuramente)
id_atendimentos_bd     = st.secrets["files_id"]["ID_ATENDIMENTOS_BD"]
id_clientes_bd         = st.secrets["files_id"]["ID_CLIENTES_BD"]
id_contratos_corban_bd = st.secrets["files_id"]["ID_CONTRATOS_CORBAN_BD"]
id_contratos_bd        = st.secrets["files_id"]["ID_CONTRATOS_BD"]
id_tabelas_bd          = st.secrets["files_id"]["ID_TABELAS_BD"]
id_telefones_bd        = st.secrets["files_id"]["ID_TELEFONES_BD"]

# ID's dos arquivos baixados via API do Corban
id_agendamentos        = st.secrets["files_id"]["ID_AGENDAMENTOS"]
id_api                 = st.secrets["files_id"]["ID_API"]
id_averbacoes          = st.secrets["files_id"]["ID_AVERBACOES"]
id_clientes            = st.secrets["files_id"]["ID_CLIENTES"]
id_comissionamentos    = st.secrets["files_id"]["ID_COMISSIONAMENTOS"]
id_comissoes           = st.secrets["files_id"]["ID_COMISSOES"]
id_contratos           = st.secrets["files_id"]["ID_CONTRATOS"]
id_datas               = st.secrets["files_id"]["ID_DATAS"]
id_documentos          = st.secrets["files_id"]["ID_DOCUMENTOS"]
id_enderecos           = st.secrets["files_id"]["ID_ENDERECOS"]
id_observacoes         = st.secrets["files_id"]["ID_OBSERVACOES"]
id_proposta            = st.secrets["files_id"]["ID_PROPOSTA"]
id_propostas           = st.secrets["files_id"]["ID_PROPOSTAS"]
id_repasse_calculado   = st.secrets["files_id"]["ID_REPASSE_CALCULADO"]
id_sinalizadores       = st.secrets["files_id"]["ID_SINALIZADORES"]
id_telefones           = st.secrets["files_id"]["ID_TELEFONES"]


clientes         = pd.read_csv(
                        f"https://drive.google.com/uc?id={id_clientes_bd}", 
                        sep=',', 
                        encoding='UTF-8', 
                        quoting=csv.QUOTE_ALL, 
                        quotechar='"', 
                        on_bad_lines='skip', 
                        engine='python')
atendimentos     = pd.read_csv(
                        f"https://drive.google.com/uc?id={id_atendimentos_bd}", 
                        sep=',', 
                        encoding='UTF-8', 
                        quoting=csv.QUOTE_ALL, 
                        quotechar='"', 
                        on_bad_lines='skip', 
                        engine='python')
telefones        = pd.read_csv(
                        f"https://drive.google.com/uc?id={id_telefones_bd}", 
                        sep=',', 
                        encoding='UTF-8', 
                        quoting=csv.QUOTE_ALL, 
                        quotechar='"', 
                        on_bad_lines='skip', 
                        engine='python'
                    )
contratos        = pd.read_csv(
                        f"https://drive.google.com/uc?id={id_contratos_bd}", 
                        sep=',', 
                        encoding='UTF-8', 
                        quoting=csv.QUOTE_ALL, 
                        quotechar='"', 
                        on_bad_lines='skip', 
                        engine='python'
                    )
contratos_corban = pd.read_csv(
                        f"https://drive.google.com/uc?id={id_contratos_corban_bd}", 
                        sep=',', 
                        encoding='UTF-8', 
                        quoting=csv.QUOTE_ALL, 
                        quotechar='"', 
                        on_bad_lines='skip', 
                        engine='python'
                    )

propostas        = pd.read_csv(
                        f"https://drive.google.com/uc?id={id_propostas}", 
                        low_memory=False
                    )
comissoes        = pd.read_csv(
                        f"https://drive.google.com/uc?id={id_comissoes}", 
                        low_memory=False
                    )
comissionamento  = pd.read_csv(
                        f"https://drive.google.com/uc?id={id_comissionamentos}", 
                        low_memory=False
                    )
clientes_corban  = pd.read_csv(
                        f"https://drive.google.com/uc?id={id_clientes}", 
                        low_memory=False
                    )
telefones_corban = pd.read_csv(
                        f"https://drive.google.com/uc?id={id_telefones}", 
                        low_memory=False
                    )

dk.register("clientes", clientes)
dk.register("atendimentos", atendimentos)
dk.register("telefones", telefones)
dk.register("contratos", contratos)
dk.register("contratos_corban", contratos_corban)

dk.register("propostas_concatenado", propostas)
dk.register("comissoes_concatenado", comissoes)
dk.register("comissionamentos_concatenado", comissionamento)
dk.register("clientes_concatenado", clientes_corban)
dk.register("telefones_concatenado", telefones_corban)

# Cria conexão com DuckDB
con = dk.connect()

# Registra o DataFrame como uma tabela no contexto do DuckDB
con.register("atendimentos", atendimentos)

class QuerysCSV:
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
    

    ##### POSTGRESQL #####
    def data_proposta_corban(self):
        query = f"""select
                        p.data_status as data
                    from propostas_concatenado p;"""
        return query
    
    def origem_proposta_corban(self):
        query = f"""select distinct
                        p.origem as origem
                    from propostas_concatenado p
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
                        p.data_status as "Data Status", 
                        p.origem as "Origem", 
                        p.status_nome as "Status", 
                        c.valor as "Valor", 
                        c.data as "Data da Comissão", 
                        co.percentual_valor_base as "Percetual Valor Base", 
                        co.valor_fixo as "Valor Fixo"
                    from propostas_concatenado p
                    left join comissoes_concatenado c
                    on p.proposta_id = c.proposta_id
                    left join comissionamentos_concatenado co
                    on p.proposta_id = co.proposta_id
                    {condicao}
                    order by p.data_status;"""
        return query
    
    #################### BASE FGTS ####################

    def consulta_base_fgts(self, condicao):
        query = f"""with consulta as (
                        select
                            c.id,
                            LPAD(CAST(c.CPF AS CHAR), 11, '0') AS CPF,
                            c.nome,
                            cb.produto,
                            cb.propostaIdBanco as numContrato,
                            cb.inclusao as dataInclusao,
                            cb.pagamento as dataPagamento,
                            cb.valorFinanciado,
                            cb.valorLiberado,
                            cb.status as status,
                            'Corban' as origem
                        from clientes c 
                        left join contratosCorban cb on c.id = cb.clienteId
                        union
                        select
                            c.id,
                            LPAD(CAST(c.CPF AS CHAR), 11, '0') AS CPF,
                            c.nome,
                            ct.produto,
                            ct.numContrato,
                            ct.dataInclusao,
                            ct.dataPagamento,
                            ct.valorFinanciado,
                            ct.valorLiberado,
                            sb.descricao as status,
                            'CRM' as origem
                        from clientes c 
                        left join contratos ct on c.id = ct.clienteId 
                        left join statusBanco sb on sb.id = ct.statusBancoId
                    )
                    select distinct
                        c.CPF,
                        c.nome,
                        c.numContrato,
                        c.dataInclusao,
                        c.dataPagamento,
                        c.valorFinanciado,
                        c.valorLiberado,
                        c.status,
                        c.origem,
                        l.telefone as telefoneLeads,
                        t.telefone as telefone
                    from consulta c 
                    left join leads l on c.id = l.clientId 
                    left join telefones t on c.id = t.clienteId 
                    where {condicao} limit 100;"""
        return query
    
    def clientes_sem_cpf(self):
        query = """SELECT 
                        c.CPF,
                        t.telefone
                    FROM clientes c 
                    RIGHT JOIN telefones t ON c.id = t.clienteId
                    WHERE c.CPF IS NULL limit 100;"""
        return query
    
    def obtem_telefones(self):
        query = """select distinct
                        LPAD(cc.cliente_cpf::TEXT, 11, '0') AS "CPF",
                        concat(tc.ddd, tc.numero) as "telefoneAPICorban" 
                    from clientes_concatenado cc  
                    left join telefones_concatenado tc on cc.cliente_id = tc.cliente_id;"""
        return query
    
    def obtem_telefones_api_corban(self):
        query = """select 
                        concat(tc.ddd, tc.numero) as telefoneAPI, 
                        LPAD(cc.cliente_cpf::TEXT, 11, '0') AS "CPF"
                    from telefones_concatenado tc 
                    left join clientes_concatenado cc on tc.cliente_id = cc.cliente_id;"""
        return query