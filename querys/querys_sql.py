class QuerysSQL:
    def __init__(self):
        ...

    ##### CLIENTES ATENDIDOS #####
    def total_clientes(self):
        query = f"""select 
                        count(distinct c.cpf) as "TOTAL_CPF"
                    from public."Clientes" c;"""
        
        return query
    
    def clientes_atendidos(self):
        query = f"""select distinct
                        date_format(l.data, '%d/%m/%Y') as "Data",
                        c.cpf as "CPF", 
                        c.nome as "Nome", 
                        l.telefone as "telefoneLead",
                        e.cidade as "Cidade", 
                        e.estado as "UF", 
                        l.etapa as "Etapa"
                    from public."Clientes" c 
                    right join public."Leads" l 
                        on c.id = l."clientId"
                    left join public."Enderecos" e 
                        on c.id = e."clientId"
                    order by l.data desc;"""
        
        return query
    
    ##### CLIENTES NOVOS #####
    def qtd_clientes_total(self):
        query = f"""select 
                        count(distinct c.cpf) as total
                    from public."Clientes" c;"""
        return query
    
    def clientes_novos(self):
        query = f"""select distinct
                        ct."dataInclusao" as "Inclusão CRM",
                        ctb.inclusao as "Inclusão Corban",
                        c.cpf as "CPF", 
                        c.nome as "Nome",
                        t.telefone as "Telefone"
                    from public."Clientes" c
                    left join public."Telefones" t
                    on c.id = t."clienteId"
                    left join public."Contratos" ct
                    on c.id = ct."clienteId"
                    left join sistema."ContratosCorban" ctb
                    on c.id = ctb."clienteId";"""
        
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
                            'public' as origem
                        from public.Contratos ct
                        left join public.StatusBanco sb on sb.id = ct.statusBancoId
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
                    right join public.Clientes c on ct.clienteId = c.id
                    left join public.Leads l on c.id = l.clientId
                    left join public.Telefones t on c.id = t.clienteId
                    where {condicao};"""
        return query
    
    def clientes_sem_cpf(self):
        query = """select distinct
                        l."clientId" as cliente_id, 
                        l.telefone as telefone
                    from public."Leads" l 
                    left join public."Telefones" t on l.telefone = t.telefone
                    where l."clientId" is null and t."clienteId" is null;"""
        return query
    
    def obtem_telefones_api_corban(self):
        query = """select 
                        concat(tc.ddd, tc.numero) as "telefoneAPI", 
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
                        cc.cliente_nome as nome_corban,
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
    
    def get_crm_consulta_mysql_aws(self):
        query = f"""SELECT 
                        cs.id AS consultaId,
                        cs.clienteId,
                        cs.updatedAt AS dataConsulta,
                        c.cpf,
                        cs.erros,
                        cs.tabela as tabelaId,
                        cs.valorLiberado,
                        cs.valorContrato,
                        b.nome as banco
                    FROM CRM.Consultas cs
                    left join CRM.Bancos b on cs.bancoId = b.id
                    left join CRM.Clientes c on cs.clienteId = c.id
                    WHERE cs.updatedAt >= '2025-11-01 00:00:00'
                    AND (c.cpf is not null OR c.cpf <> '');"""
        return query
    
    def get_crm_consulta_postgres_aws(self):
        query = f"""SELECT 
                        cs.id AS "consultaId",
                        cs."clienteId",
                        cs."updatedAt" AS "dataConsulta",
                        c.cpf,
                        cs.erros,
                        cs."tabelaId",
                        cs."valorLiberado",
                        cs."valorBruto" as "valorContrato",
                        b.nome as banco
                    FROM public."Consultas" cs
                    left join public."Bancos" b on cs."bancoId" = b.id
                    left join public."Clientes" c on cs."clienteId" = c.id
                    WHERE cs."updatedAt" >= '2025-11-01 00:00:00'
                    AND (c.cpf is not null OR c.cpf <> '');"""
        return query
    
    def get_crm_cliente_mysql_aws(self):
        query = f"""SELECT 
                        cl.id AS clienteId,
                        cl.cpf,
                        cl.nome
                    FROM CRM.Clientes cl;"""
        return query
    
    def get_crm_cliente_postgres_aws(self):
        query = f"""SELECT 
                        cl.id AS "clienteId",
                        cl.cpf,
                        cl.nome
                    FROM public."Clientes" cl;"""
        return query
    
    def get_crm_telefone_mysql_aws(self):
        query = f"""WITH fone_sistema AS (
                        SELECT 
                            cl.id, 
                            ts.telefone 
                        FROM CRM.Clientes cl 
                        LEFT JOIN CRM.Telefones ts 
                            ON cl.id = ts.clienteId
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
    
    def get_crm_telefone_postgres_aws(self):
        query = f"""WITH fone_sistema AS (
                        SELECT 
                            cl.id, 
                            ts.numero as telefone 
                        FROM public."Clientes" cl 
                        LEFT JOIN public."Telefones" ts 
                            ON cl.id = ts."clienteId"
                    )
                    SELECT DISTINCT *
                    FROM (
                        SELECT 
                            fs.id AS "clienteId", 
                            CASE
                                WHEN fs.telefone IS NULL THEN tc.numero
                                WHEN fs.telefone <> tc.numero THEN tc.numero
                                when tc.numero is null then fs.telefone
                            END AS telefone_crm
                        FROM fone_sistema fs 
                        LEFT JOIN public."Telefones" tc 
                            ON fs.id = tc."clienteId"
                    ) AS resultado
                    WHERE telefone_crm IS not NULL;"""
        return query
    
    def get_sistema_lead_mysql_aws(self):
        query = f"""SELECT 
                        l.consultaId,
                        l.clientId AS clienteId,
                        l.telefone AS telefone_lead
                    FROM sistema.Leads l
                    WHERE l.consultaId IS NOT NULL 
                        and l.clientId IS NOT NULL;"""
        return query
    
    def get_crm_lead_mysql_aws(self):
        query = f"""SELECT 
                        l.consultaId,
                        l.clientId AS clienteId,
                        l.telefone AS telefone_lead
                    FROM CRM.Leads l
                    WHERE l.consultaId IS NOT NULL 
                        and l.clientId IS NOT NULL;"""
        return query
    
    def get_crm_autoatendimento_postgres_aws(self):
        query = f"""SELECT 
                        l."simulacaoCltId" as "consultaId", 
                        l."clienteId",
                        l.telefone AS telefone_lead
                    FROM public."AutoAtendimento" l
                    WHERE l."simulacaoCltId" IS NOT null
                        and l."clienteId" IS NOT NULL;"""
        return query
    
    def get_crm_tabela_mysql_aws(self):
        query = f"""SELECT 
                        tb.id AS tabelaId,
                        tb.nome AS tabela
                    FROM CRM.Tabelas tb;"""
        return query
    
    def get_crm_tabela_postgres_aws(self):
        query = f"""SELECT 
                        CAST(tb.id AS integer) AS "tabelaId",
                        tb.nome AS tabela
                    FROM public."Tabelas" tb;"""
        return query
    
    def get_crm_parcela_mysql_aws(self):
        query = f"""SELECT 
                        pc.consultaId,
                        pc.num AS parcelas
                    FROM CRM.Parcelas pc;"""
        return query
    
    def get_crm_parcela_postgres_aws(self):
        query = f"""SELECT 
                        pc."consultaId",
                        pc.numero AS parcelas
                    FROM public."Parcelamento" pc;"""
        return query
    
    def get_base_consolidada(self):
        query = f"""SELECT distinct
                        cpf as cpf_consolidado,
                        telefone as telefone_consolidado,
                        nome as nome_consolidado
                    FROM extracoes.base_consolidada;"""
        return query
    
    ##### CAMPANHAS #####

    def get_campanhas(self):
        query_digisac = f"""
                        select 
                            c.nome as nome_digisac,
                            c.nome_interno,
                            c.telefone as numero,
                            LPAD(REGEXP_REPLACE(c.cpf, '\D', '', 'g')::text, 11, '0') AS cpf_digisac,
                            m.dt_mensagem,
                            m.dsc_valor as banco,
                            m.msg_inicial
                        from public.clientes c 
                        left join public.mensagens m 
                            on c.telefone = m.telefone 
                        where m.campo_personalizado = 'CPF_aprovado'
                            and m.dt_mensagem >= '2026-01-05 00:00:00.000';
                        """
        query_corban = f"""
                        select 
                            c.cliente_nome as nome,
                            LPAD(REGEXP_REPLACE(c.cliente_cpf, '\D', '', 'g')::text, 11, '0') as cpf_corban,
                            CASE
                                WHEN LENGTH(concat(t.ddd,t.numero)) < 11
                                THEN SUBSTRING(concat(t.ddd,t.numero) FROM 1 FOR 2) || '9' || SUBSTRING(concat(t.ddd,t.numero) FROM 3)
                                ELSE concat(t.ddd,t.numero)
                            END AS numero,
                            a.data_status_api as liberacao,
                            ct.valor_financiado,
                            ct.valor_liberado,
                            ct.valor_parcela,
                            ct.prazo,
                            ct.banco_nome as nome_banco,
                            cc.comissao_valor_liberado as valor_comissao
                        from corban.clientes c 
                        left join corban.telefones t 
                            on c.cliente_id = t.cliente_id
                        left join corban.contrato ct
                            on c.cliente_id = ct.cliente_id 
                        left join corban.propostas p
                            on ct.proposta_id = p.proposta_id 
                        left join corban.api a
                            on ct.proposta_id = a.proposta_id
                        left join corban.comissionamentos cc
                            on ct.proposta_id = cc.proposta_id
                        where p.status_nome like '%Pago%';
                        """
        # query_crm = f"""
        #             select 
        #                 c.nome as nome,
        #                 c.cpf as cpf,
        #                 t.numero as telefone,
        #                 p."dataPagamento" as liberacao,
        #                 p."valorBruto" as valor_financiado,
        #                 p."valorLiberado" as valor_liberado,
        #                 (p."valorLiberado" / p.prazo) as valor_parcela,
        #                 p.prazo,
        #                 b.nome as nome_banco,
        #                 p."valorTotalComissao" as valor_comissao
        #             from public."Propostas" p
        #             left join public."Clientes" c
        #                 on p."clienteId" = c.id
        #             left join public."Telefones" t
        #                 on c.id = t."clienteId" 
        #             left join public."Bancos" b
        #                 on p."bancoId" = b.id
        #             where p."usuarioId" = '29' 
        #                 and p."dataPagamento" >= '2026-01-05 00:00:00.000';
        #             """
        query_crm = f"""
                    select 
                        aa.telefone as numero,
                        c.cpf,
                        c.nome,
                        aa."createdAt",
                        aa."mensagemInicial",
                        b.nome as nome_banco,
                        p."dataPagamento",
                        case 
                            when p."dataPagamento" is not null then cs."valorBruto"
                        end as "valorBruto",
                        case
                            when p."dataPagamento" is not null then p."valorLiberado"
                        end as "valorLiberado",
                        case 
                            when p."dataPagamento" is not null then (cast(cs."valorBruto" as float) / p.prazo)
                        end as valor_parcela,
                        case 
                            when p."dataPagamento" is not null then p.prazo 
                        end as prazo,
                        case
                            when p."valorTotalComissao" = 0 then p."valorLiberado"*0.09
	                        when p."dataPagamento" is not null then p."valorTotalComissao"
                        end as "valorTotalComissao"
                    from "Clientes" c
                    full outer join "AutoAtendimento" aa
                        on c.id = aa."clienteId"
                    left join "Consultas" cs
                        on aa."simulacaoCltId" = cs.id
                    left join "Bancos" b
                        on cs."bancoId" = b.id
                    left join "Propostas" p
                        on cs.id = p."consultaId"
                    left join "Telefones" t
                        on c.id = t."clienteId"
                    where aa."createdAt" >= '2026-01-05 00:00:00.000';
                """
        return query_digisac, query_corban, query_crm
        # return query
        
    def get_campanhas_meta(self):
        query = """
                SELECT * FROM controle.campanhas;
                """
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
                    FROM public.Consultas cs
                    left join public.Parcelas pc on cs.id = pc.consultaId
                    left join public.Tabelas tb on cs.tabela = tb.id
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
                    FROM public.Consultas cs
                    left join public.Parcelas pc on cs.id = pc.consultaId
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
                    FROM public.Consultas cs
                    left join public.Tabelas tb on cs.tabela = tb.id
                    WHERE cs.updatedAt >= '2025-11-01 00:00:00'
                    AND (cs.CPF is not null OR cs.CPF <> '');"""
    
    def get_telefone(self):
        return ""