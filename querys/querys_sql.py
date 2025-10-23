from datetime import datetime

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
                        date_format(a.data, '%d/%m/%Y') as Data,
                        c.CPF as CPF, 
                        c.nome as Nome, 
                        t.telefone as Telefone, 
                        c.cidade as Cidade, 
                        c.estado as UF, 
                        a.chatId as "Chat ID", 
                        a.etapa as Etapa, 
                        a.status as Status
                    from sistema.Clientes c 
                    inner join sistema.Atendimentos a 
                    on c.id = a.clienteId
                    inner join sistema.Telefones t
                    on c.id = t.clienteId
                    {condicao}
                    order by a.data desc;;"""
        
        return query
    
    def total_clientes(self):
        query = f"""select distinct 
                        c.CPF as TOTAL_CPF
                    from sistema.Clientes c 
                    inner join sistema.Atendimentos a 
                    on c.id = a.clienteId
                    inner join sistema.Telefones t
                    on c.id = t.clienteId;"""
        
        return query
    
    def status_atendimentos(self):
        query = '''select 
                        a.status as Status
                    from sistema.Atendimentos a;'''
        
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
                        date_format(a.data, '%d/%m/%Y') as Data,
                        c.CPF as CPF, 
                        c.nome as Nome, 
                        t.telefone as Telefone, 
                        c.cidade as Cidade, 
                        c.estado as UF, 
                        a.chatId as "Chat ID", 
                        a.etapa as Etapa, 
                        a.status as Status
                    from sistema.Clientes c 
                    inner join sistema.Atendimentos a 
                    on c.id = a.clienteId
                    inner join sistema.Telefones t
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
                        from sistema.Clientes c 
                        inner join sistema.Atendimentos a 
                        on c.id = a.clienteId
                        inner join sistema.Telefones t
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
                    from sistema.Clientes c
                    inner join sistema.Telefones t
                    on c.id = t.clienteId
                    left join sistema.Contratos ct
                    on c.id = ct.clienteId
                    left join sistema.ContratosCorban ctb
                    on c.id = ctb.clienteId
                    {condicao};"""
        
        return query
    
    def qtd_clientes_novos(self):
        query = f"""select distinct
                    c.CPF as CPF
                from sistema.Clientes c
                inner join sistema.Telefones t
                on c.id = t.clienteId
                left join sistema.Contratos ct
                on c.id = ct.clienteId
                left join sistema.ContratosCorban ctb
                on c.id = ctb.clienteId
                where ct.prazo is null
                and ctb.prazo is null;"""
        return query
    
    def qtd_clientes(self):
        query = f"""select distinct
                        c.CPF as CPF
                    from sistema.Clientes c;"""
        return query
    

    ##### POSTGRESQL #####
    def data_proposta_corban(self):
        query = f"""select
                        p.data_status as data
                    from "propostasCorban".propostas_concatenado p;"""
        return query
    
    def origem_proposta_corban(self):
        query = f"""select distinct
                        p.origem as origem
                    from "propostasCorban".propostas_concatenado p
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
                    from "propostasCorban".propostas_concatenado p
                    left join "propostasCorban".comissoes_concatenado c
                    on p.proposta_id = c.proposta_id
                    left join "propostasCorban".comissionamentos_concatenado co
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
                        from sistema.Clientes c 
                        left join sistema.ContratosCorban cb on c.id = cb.clienteId
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
                        from sistema.Clientes c 
                        left join sistema.Contratos ct on c.id = ct.clienteId 
                        left join sistema.StatusBanco sb on sb.id = ct.statusBancoId
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
                    left join sistema.Leads l on c.id = l.clientId 
                    left join sistema.Telefones t on c.id = t.clienteId 
                    where {condicao} limit 100;"""
        return query
    
    def clientes_sem_cpf(self):
        query = """SELECT 
                        c.CPF,
                        t.telefone
                    FROM sistema.Clientes c 
                    RIGHT JOIN sistema.Telefones t ON c.id = t.clienteId
                    WHERE c.CPF IS NULL limit 100;"""
        return query
    
    def obtem_telefones(self):
        query = """select distinct
                        LPAD(cc.cliente_cpf::TEXT, 11, '0') AS "CPF",
                        concat(tc.ddd, tc.numero) as "telefoneAPICorban" 
                    from "propostasCorban".clientes_concatenado cc  
                    left join "propostasCorban".telefones_concatenado tc on cc.cliente_id = tc.cliente_id;"""
        return query
    
    def obtem_telefones_api_corban(self):
        query = """select 
                        concat(tc.ddd, tc.numero) as telefoneAPI, 
                        LPAD(cc.cliente_cpf::TEXT, 11, '0') AS "CPF"
                    from "propostasCorban".telefones_concatenado tc 
                    left join "propostasCorban".clientes_concatenado cc on tc.cliente_id = cc.cliente_id;"""
        return query