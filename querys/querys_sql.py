class QuerysSQL:
    def __init__(self):
        ...

    ##### CLIENTES ATENDIDOS #####
    def total_clientes(self):
        query = f"""select 
                        count(distinct c.CPF) as TOTAL_CPF
                    from CRM.Clientes c;"""
        
        return query
    
    def clientes_atendidos(self):
        query = f"""select distinct
                        date_format(l.data, '%d/%m/%Y') as "Data",
                        c.CPF as "CPF", 
                        c.nome as Nome, 
                        l.telefone as telefoneLead,
                        e.cidade as Cidade, 
                        e.estado as UF, 
                        l.etapa as Etapa
                    from CRM.Clientes c 
                    right join CRM.Leads l 
                        on c.id = l.clientId
                    left join CRM.Enderecos e 
                        on c.id = e.clientId
                    order by l.data desc;"""
        
        return query
    
    ##### CLIENTES NOVOS #####
    def qtd_clientes_total(self):
        query = f"""select 
                        count(distinct c.CPF) as total
                    from CRM.Clientes c;"""
        return query
    
    def clientes_novos(self):
        query = f"""select distinct
                        ct.dataInclusao as "Inclusão CRM",
                        ctb.inclusao as "Inclusão Corban",
                        c.CPF as CPF, 
                        c.nome as Nome,
                        t.telefone as Telefone
                    from CRM.Clientes c
                    left join CRM.Telefones t
                    on c.id = t.clienteId
                    left join CRM.Contratos ct
                    on c.id = ct.clienteId
                    left join sistema.ContratosCorban ctb
                    on c.id = ctb.clienteId;"""
        
        return query
    
    ##### DISPAROS REALIZADOS #####
    def disparos(self):
        query = """select
                        "CPF" as cpf_disparos,
                        telefone as "telefone_disparos",
                        valor,
                        status,
                        data as "dataDisparos",
                        vendedor as "vendedorDisparos"
                    from "extracoes".disparos;"""
        return query

    #################### BASE FGTS ####################
    def obtem_telefones(self):
        query = """select distinct
                        LPAD(cc.cliente_cpf::TEXT, 11, '0') AS "CPF",
                        concat(tc.ddd, tc.numero) as "telefoneAPICorban" 
                    from corban.clientes cc  
                    left join corban.telefones tc on cc.cliente_id = tc.cliente_id;"""
        return query
    
    def consulta_base_fgts(self, condicao):
        query = f"""with contratos as (
                        select
                            cb.clienteId,
                            cb.produto,
                            cb.propostaIdBanco as numContrato,
                            cb.inclusao as dataInclusao,
                            cb.pagamento as dataPagamento,
                            cb.valorFinanciado,
                            cb.valorLiberado,
                            cb.status as status,
                            'Corban' as origem
                        from sistema.ContratosCorban cb
                        union
                        select
                            ct.clienteId,
                            ct.produto,
                            ct.numContrato,
                            ct.dataInclusao,
                            ct.dataPagamento,
                            ct.valorFinanciado,
                            ct.valorLiberado,
                            sb.descricao as status,
                            'CRM' as origem
                        from CRM.Contratos ct
                        left join CRM.StatusBanco sb on sb.id = ct.statusBancoId
                    )
                    select 
                        c.CPF,
                        c.nome,
                        ct.numContrato as numContrato,
                        ct.dataInclusao,
                        ct.dataPagamento,
                        ct.valorFinanciado,
                        ct.valorLiberado,
                        ct.status,
                        ct.origem,
                        l.telefone as telefoneLead, 
                        t.telefone as telefone
                    from contratos ct
                    right join CRM.Clientes c on ct.clienteId = c.id
                    left join CRM.Leads l on c.id = l.clientId
                    left join CRM.Telefones t on c.id = t.clienteId
                    where {condicao};"""
        return query
    
    def clientes_sem_cpf(self):
        query = """select distinct
                        l.clientId as cliente_id, 
                        l.telefone as telefone
                    from CRM.Leads l 
                    left join CRM.Telefones t on l.telefone = t.telefone
                    where l.clientId is null and t.clienteId is null;"""
        return query
    
    def obtem_telefones_api_corban(self):
        query = """select 
                        concat(tc.ddd, tc.numero) as telefoneAPI, 
                        LPAD(cc.cliente_cpf::TEXT, 11, '0') AS "CPF"
                    from corban.telefones tc 
                    left join corban.clientes cc on tc.cliente_id = cc.cliente_id;"""
        return query
    
    ##### COMISSÕES CORBAN #####
    def api(self):
        query = """select
                        ac.proposta_id,
                        ac.data_status_api,
                        ac.status_api
                    from corban.api ac ;"""
        return query

    def comissionamento(self):
        query = """select
                        cc2.proposta_id,
                        CASE
                            WHEN cc2.recebe_valor_base ~ '^[0-9,.]+$' THEN
                                REPLACE(cc2.recebe_valor_base, ',', '.')::NUMERIC
                            ELSE 0
                        end as recebe_valor_base
                    from corban.comissionamentos cc2 ;"""
        return query

    def comissoes(self):
        query = """select
                        cc.data,
                        cc.valor,
                        cc.proposta_id
                    from corban.comissoes cc ;"""
        return query

    def datas(self):
        query = """select
                        dc.cancelado,
                        dc.pagamento,
                        dc.proposta_id
                    from corban.datas dc ;"""
        return query

    def proposta(self):
        query = """select
                        pc.proposta_id,
                        pc.valor_total_comissionado
                    from corban.contrato pc ;"""
        return query

    def propostas(self):
        query = """select 
                        psc.data_status,
                        psc.origem,
                        psc.proposta_id,
                        psc.status_nome
                    from corban.propostas psc;"""
        return query


    def get_digisac(self):
        query = f"""select
                        cpf as cpf_digisac,
                        nome_interno,
                        telefone as telefone_digisac,
                        data,
                        falha
                    from "extracoes".digisac
                    where data >= '2025-11-01';"""
        return query
    
    def get_corban(self):
        query = f"""select 
                        ac.status_api,
                        cc.cliente_cpf as cpf_corban,
                        concat(tc.ddd,tc.numero) as telefone_propostas,
                        ac.data_atualizacao_api 
                    from corban.api ac
                    left join corban.contrato pc on ac.proposta_id = pc.proposta_id 
                    left join corban.clientes cc on pc.cliente_id = cc.cliente_id
                    left join corban.telefones tc on cc.cliente_id = tc.cliente_id
                    where pc.produto_id = 13;"""
        return query
    
    def get_telefones_corban(self):
        query = f"""select 
                        cc.cliente_cpf as cpf_telefone_corban, 
                        concat(tc.ddd,tc.numero) as telefone_corban 
                    from corban.telefones tc
                    right join corban.clientes cc on cc.cliente_id = tc.cliente_id;"""
        return query
    
    def get_crm_consulta(self):
        query = f"""SELECT 
                        cs.id AS consultaId,
                        cs.clienteId,
                        cs.updatedAt AS dataConsulta,
                        cs.CPF AS cpf,
                        cs.erros,
                        cs.tabela AS tabelaId,
                        cs.valorLiberado,
                        cs.valorContrato
                    FROM CRM.Consultas cs
                    WHERE cs.updatedAt >= '2025-11-01 00:00:00'
                    AND (cs.CPF is not null OR cs.CPF <> '');"""
        return query
    
    def get_crm_cliente(self):
        query = f"""SELECT 
                        cl.id AS clienteId,
                        cl.identificador,
                        cl.CPF AS cpf,
                        cl.nome
                    FROM CRM.Clientes cl 
                    WHERE cl.CPF NOT IN ('12979230901', '10101201907', '04512025111', '10101215614', '4348724075');"""
        return query
    
    def get_crm_telefone(self):
        query = f"""WITH fone_sistema AS (
                        SELECT 
                            cl.id, 
                            ts.telefone 
                        FROM CRM.Clientes cl 
                        LEFT JOIN sistema.Telefones ts 
                            ON cl.identificador = ts.clienteId
                    )
                    SELECT DISTINCT *
                    FROM (
                        SELECT 
                            fs.id AS clienteId, 
                            CASE
                                WHEN fs.telefone IS NULL THEN tc.telefone
                                WHEN fs.telefone <> tc.telefone THEN tc.telefone
                                when tc.telefone is null then fs.telefone
                            END AS telefone_crm
                        FROM fone_sistema fs 
                        LEFT JOIN CRM.Telefones tc 
                            ON fs.id = tc.clienteId
                    ) AS resultado
                    WHERE telefone_crm IS not NULL;"""
        return query
    
    def get_crm_lead(self):
        query = f"""SELECT 
                        l.consultaId AS consultaId,
                        l.clientId AS clienteId,
                        l.telefone AS telefone_lead
                    FROM CRM.Leads l
                    WHERE l.consultaId IS NOT NULL 
                        OR l.clientId IS NOT NULL;"""
        return query
    
    def get_crm_tabela(self):
        query = f"""SELECT 
                        CAST(tb.id AS SIGNED) AS tabelaId,
                        tb.nome AS tabela
                    FROM CRM.Tabelas tb;"""
        return query
    
    def get_crm_parcela(self):
        query = f"""SELECT 
                        pc.consultaId AS consultaId,
                        pc.num AS parcelas
                    FROM CRM.Parcelas pc;"""
        return query
    
    def get_base_consolidada(self):
        query = f"""SELECT distinct
                        cpf as cpf_consolidado,
                        telefone as telefone_consolidado,
                        nome as nome_consolidado
                    FROM extracoes.base_consolidada;"""
        return query
    


    #####################################################################
    def get_teste(self):
        return """SELECT 
                        CONVERT(cs.id, SIGNED)     AS consultaId,
                        CONVERT(cs.clienteId, SIGNED)   AS clienteId,
                        '' as identificador,
                        cs.updatedAt           AS dataConsulta,
                        cs.CPF                 AS cpf,
                        cs.erros,
                        CONVERT(cs.tabela, SIGNED) AS tabelaId,
                        cs.valorLiberado,
                        cs.valorContrato,
                        CONVERT(pc.consultaId, SIGNED) AS consultaIdParcela,
                        pc.num         AS parcelas,
                        CONVERT(tb.id, SIGNED) AS tabelaIdTabela,
                        tb.nome AS tabela
                    FROM CRM.Consultas cs
                    left join CRM.Parcelas pc on cs.id = pc.consultaId
                    left join CRM.Tabelas tb on cs.tabela = tb.id
                    WHERE cs.updatedAt >= '2025-11-01 00:00:00'
                    AND (cs.CPF is not null OR cs.CPF <> '');"""
    
    def get_parcelas(self):
        return """SELECT 
                        CONVERT(cs.id, SIGNED)     AS consultaId,
                        CONVERT(cs.clienteId, SIGNED)   AS clienteId,
                        '' as identificador,
                        cs.updatedAt           AS dataConsulta,
                        cs.CPF                 AS cpf,
                        cs.erros,
                        CONVERT(cs.tabela, SIGNED) AS tabelaId,
                        cs.valorLiberado,
                        cs.valorContrato,
                        CAST(pc.consultaId AS SIGNED) AS consultaId,
                        pc.num         AS parcelas
                    FROM CRM.Consultas cs
                    left join CRM.Parcelas pc on cs.id = pc.consultaId
                    WHERE cs.updatedAt >= '2025-11-01 00:00:00'
                    AND (cs.CPF is not null OR cs.CPF <> '')
                    and cs.clienteId is not null
                    order by cs.CPF asc, cs.updatedAt desc;"""
    
    def get_tabelas(self):
        return """SELECT 
                        CONVERT(cs.id, SIGNED)     AS consultaId,
                        CONVERT(cs.clienteId, SIGNED)   AS clienteId,
                        cs.updatedAt           AS dataConsulta,
                        cs.CPF                 AS cpf,
                        cs.erros,
                        CONVERT(cs.tabela, SIGNED) AS tabelaId,
                        cs.valorLiberado,
                        cs.valorContrato,
                        CAST(tb.id AS SIGNED) AS tabelaId,
                        tb.nome AS tabela
                    FROM CRM.Consultas cs
                    left join CRM.Tabelas tb on cs.tabela = tb.id
                    WHERE cs.updatedAt >= '2025-11-01 00:00:00'
                    AND (cs.CPF is not null OR cs.CPF <> '');"""
    
    def get_telefone(self):
        return ""