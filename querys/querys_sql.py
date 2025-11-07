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
                        "CPF",
                        telefone,
                        valor,
                        status,
                        data,
                        vendedor
                    from "extracoes".disparos;"""
        return query

    #################### BASE FGTS ####################
    def obtem_telefones(self):
        query = """select distinct
                        LPAD(cc.cliente_cpf::TEXT, 11, '0') AS "CPF",
                        concat(tc.ddd, tc.numero) as "telefoneAPICorban" 
                    from "propostasCorban".clientes_concatenado cc  
                    left join "propostasCorban".telefones_concatenado tc on cc.cliente_id = tc.cliente_id;"""
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
                    from "propostasCorban".telefones_concatenado tc 
                    left join "propostasCorban".clientes_concatenado cc on tc.cliente_id = cc.cliente_id;"""
        return query
    
    ##### COMISSÕES CORBAN #####
    def api(self):
        query = """select
                        ac.proposta_id,
                        ac.data_status_api,
                        ac.status_api
                    from "propostasCorban".api_concatenado ac ;"""
        return query

    def comissionamento(self):
        query = """select
                        cc2.proposta_id,
                        CASE
                            WHEN cc2.recebe_valor_base ~ '^[0-9,.]+$' THEN
                                REPLACE(cc2.recebe_valor_base, ',', '.')::NUMERIC
                            ELSE 0
                        end as recebe_valor_base
                    from "propostasCorban".comissionamentos_concatenado cc2 ;"""
        return query

    def comissoes(self):
        query = """select
                        cc.data,
                        cc.valor,
                        cc.proposta_id
                    from "propostasCorban".comissoes_concatenado cc ;"""
        return query

    def datas(self):
        query = """select
                        dc.cancelado,
                        dc.pagamento,
                        dc.proposta_id
                    from "propostasCorban".datas_concatenado dc ;"""
        return query

    def proposta(self):
        query = """select
                        pc.proposta_id,
                        pc.valor_total_comissionado
                    from "propostasCorban".proposta_concatenado pc ;"""
        return query

    def propostas(self):
        query = """select 
                        psc.data_status,
                        psc.origem,
                        psc.proposta_id,
                        psc.status_nome
                    from "propostasCorban".propostas_concatenado psc;"""
        return query


    def get_digisac(self):
        query = f"""select
                        cpf,
                        nome_interno,
                        telefone,
                        data,
                        falha
                    from "extracoes".digisac
                    where data >= '2025-11-01';"""
        return query
    
    def get_corban(self):
        query = f"""select 
                        ac.status_api,
                        cc.cliente_cpf as cpf,
                        concat(tc.ddd,tc.numero) as "telefonePropostas",
                        ac.data_atualizacao_api 
                    from "propostasCorban".api_concatenado ac
                    left join "propostasCorban".proposta_concatenado pc on ac.proposta_id = pc.proposta_id 
                    left join "propostasCorban".clientes_concatenado cc on pc.cliente_id = cc.cliente_id
                    left join "propostasCorban".telefones_concatenado tc on cc.cliente_id = tc.cliente_id
                    where ac.data_atualizacao_api >= '2025-11-01 00:00:00';"""
        return query
    
    def get_telefones_corban(self):
        query = f"""select 
                        cc.cliente_cpf as cpf, 
                        concat(tc.ddd,tc.numero) as "telefoneCorban" 
                    from "propostasCorban".telefones_concatenado tc
                    right join "propostasCorban".clientes_concatenado cc on cc.cliente_id = tc.cliente_id;"""
        return query
    
    # def get_crm(self):
    #     query = f"""SELECT 
    #                     cs.updatedAt        AS dataConsulta,
    #                     cl.CPF              AS cpf,
    #                     cl.nome,
    #                     t.telefone,
    #                     l.telefone          AS telefoneLead,
    #                     cs.erros,
    #                     tb.nome             AS tabela,
    #                     MAX(pc.num)         AS parcelas,
    #                     cs.valorLiberado,
    #                     cs.valorContrato
    #                 FROM CRM.Consultas cs
    #                 RIGHT JOIN CRM.Clientes cl 
    #                     ON cl.CPF = cs.CPF
    #                 LEFT JOIN CRM.Parcelas pc 
    #                     ON cs.id = pc.consultaId
    #                 LEFT JOIN CRM.Telefones t 
    #                     ON cl.id = t.clienteId
    #                 LEFT JOIN CRM.Tabelas tb 
    #                     ON cs.tabela = tb.id
    #                 LEFT JOIN CRM.Leads l 
    #                     ON (l.consultaId = cs.id OR l.clientId = cl.id)
    #                 WHERE cs.CPF NOT IN (12979230901, 10101201907, 04512025111, 10101215614)
    #                 AND cl.CPF NOT IN (4348724075)
    #                 AND cs.updatedAt >= '2025-11-01 00:00:00'
    #                 GROUP BY 
    #                     cs.updatedAt,
    #                     cl.id,
    #                     cl.nome,
    #                     cl.CPF,
    #                     t.telefone,
    #                     cs.erros,
    #                     cs.tabela,
    #                     cs.valorLiberado,
    #                     cs.valorContrato;"""
    #     return query
    
    def get_crm_consulta(self):
        query = f"""SELECT 
                        CONVERT(cs.id, SIGNED)     AS consultaId,
                        cs.updatedAt           AS dataConsulta,
                        cs.CPF                 AS cpf,
                        cs.erros,
                        CONVERT(cs.tabela, SIGNED) AS tabelaId,
                        cs.valorLiberado,
                        cs.valorContrato
                    FROM CRM.Consultas cs
                    WHERE cs.CPF NOT IN ('12979230901', '10101201907', '04512025111', '10101215614', '4348724075')
                    AND cs.updatedAt >= '2025-11-01 00:00:00'
                    AND (cs.CPF is not null AND cs.CPF <> '');"""
        return query
    
    def get_crm_cliente(self):
        query = f"""SELECT 
                        CAST(cl.id AS SIGNED) AS clienteId,
                        cl.CPF             AS cpf,
                        cl.nome
                    FROM CRM.Clientes cl 
                    WHERE cl.CPF NOT IN ('12979230901', '10101201907', '04512025111', '10101215614', '4348724075');"""
        return query
    
    def get_crm_telefone(self):
        query = f"""SELECT 
                        CAST(t.clienteId AS SIGNED) AS clienteId,
                        t.telefone
                    FROM CRM.Telefones t;"""
        return query
    
    def get_crm_lead(self):
        query = f"""SELECT 
                        CAST(l.consultaId AS SIGNED) AS consultaId,
                        CAST(l.clientId AS SIGNED)   AS clienteId,
                        l.telefone     AS telefoneLead
                    FROM CRM.Leads l;"""
        return query
    
    def get_crm_tabela(self):
        query = f"""SELECT 
                        CAST(tb.id AS SIGNED) AS tabelaId,
                        tb.nome AS tabela
                    FROM CRM.Tabelas tb;"""
        return query
    
    def get_crm_parcela(self):
        query = f"""SELECT 
                        CAST(pc.consultaId AS SIGNED) AS consultaId,
                        pc.num         AS parcelas
                    FROM CRM.Parcelas pc;"""
        return query
    