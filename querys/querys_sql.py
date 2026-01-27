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
                    from unificados.clientes cc  
                    left join unificados.telefones tc on cc.cliente_id = tc.cliente_id;"""
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
                    from unificados.telefones tc 
                    left join unificados.clientes cc on tc.cliente_id = cc.cliente_id;"""
        return query
    
    ##### COMISSÕES CORBAN #####
    def api(self):
        query = """select
                        ac.proposta_id,
                        ac.data_status_api,
                        ac.status_api
                    from unificados.api ac ;"""
        return query

    def comissionamento(self):
        query = """select
                        cc2.proposta_id,
                        CASE
                            WHEN cc2.recebe_valor_base ~ '^[0-9,.]+$' THEN
                                REPLACE(cc2.recebe_valor_base, ',', '.')::NUMERIC
                            ELSE 0
                        end as recebe_valor_base
                    from unificados.comissionamentos cc2 ;"""
        return query

    def comissoes(self):
        query = """select
                        cc.data,
                        cc.valor,
                        cc.proposta_id
                    from unificados.comissoes cc ;"""
        return query

    def datas(self):
        query = """select
                        dc.cancelado,
                        dc.pagamento,
                        dc.proposta_id
                    from unificados.datas dc ;"""
        return query

    def proposta(self):
        query = """select
                        pc.proposta_id,
                        pc.valor_total_comissionado
                    from unificados.contrato pc ;"""
        return query

    def propostas(self):
        query = """select 
                        psc.data_status,
                        psc.origem,
                        psc.proposta_id,
                        psc.status_nome
                    from unificados.propostas psc;"""
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
                    from unificados.api ac
                    left join unificados.contrato pc on ac.proposta_id = pc.proposta_id 
                    left join unificados.clientes cc on pc.cliente_id = cc.cliente_id
                    left join unificados.telefones tc on cc.cliente_id = tc.cliente_id
                    where pc.produto_id = 13;"""
        return query
    
    def get_telefones_corban(self):
        query = f"""select 
                        cc.cliente_cpf as cpf_telefone_corban, 
                        concat(tc.ddd,tc.numero) as telefone_corban 
                    from unificados.telefones tc
                    right join unificados.clientes cc 
                        on cc.cliente_id = tc.cliente_id;"""
        return query
    
    # def get_crm_consulta_mysql_aws(self):
    #     query = f"""SELECT 
    #                     cs.id AS consultaId,
    #                     cs.clienteId,
    #                     cs.updatedAt AS dataConsulta,
    #                     c.cpf,
    #                     cs.erros,
    #                     cs.tabela as tabelaId,
    #                     cs.valorLiberado,
    #                     cs.valorContrato,
    #                     b.nome as banco
    #                 FROM CRM.Consultas cs
    #                 left join CRM.Bancos b on cs.bancoId = b.id
    #                 left join CRM.Clientes c on cs.clienteId = c.id
    #                 WHERE cs.updatedAt >= '2025-11-01 00:00:00'
    #                 AND (c.cpf is not null OR c.cpf <> '');"""
    #     return query
    
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
    
    # def get_crm_cliente_mysql_aws(self):
    #     query = f"""SELECT 
    #                     cl.id AS clienteId,
    #                     cl.cpf,
    #                     cl.nome
    #                 FROM CRM.Clientes cl;"""
    #     return query
    
    def get_crm_cliente_postgres_aws(self):
        query = f"""SELECT 
                        cl.id AS "clienteId",
                        cl.cpf,
                        cl.nome
                    FROM public."Clientes" cl;"""
        return query
    
    # def get_crm_telefone_mysql_aws(self):
    #     query = f"""WITH fone_sistema AS (
    #                     SELECT 
    #                         cl.id, 
    #                         ts.telefone 
    #                     FROM CRM.Clientes cl 
    #                     LEFT JOIN CRM.Telefones ts 
    #                         ON cl.id = ts.clienteId
    #                 )
    #                 SELECT DISTINCT *
    #                 FROM (
    #                     SELECT 
    #                         fs.id AS clienteId, 
    #                         CASE
    #                             WHEN fs.telefone IS NULL THEN tc.telefone
    #                             WHEN fs.telefone <> tc.telefone THEN tc.telefone
    #                             when tc.telefone is null then fs.telefone
    #                         END AS telefone_crm
    #                     FROM fone_sistema fs 
    #                     LEFT JOIN CRM.Telefones tc 
    #                         ON fs.id = tc.clienteId
    #                 ) AS resultado
    #                 WHERE telefone_crm IS not NULL;"""
    #     return query
    
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
    
    # def get_sistema_lead_mysql_aws(self):
    #     query = f"""SELECT 
    #                     l.consultaId,
    #                     l.clientId AS clienteId,
    #                     l.telefone AS telefone_lead
    #                 FROM sistema.Leads l
    #                 WHERE l.consultaId IS NOT NULL 
    #                     and l.clientId IS NOT NULL;"""
    #     return query
    
    # def get_crm_lead_mysql_aws(self):
    #     query = f"""SELECT 
    #                     l.consultaId,
    #                     l.clientId AS clienteId,
    #                     l.telefone AS telefone_lead
    #                 FROM CRM.Leads l
    #                 WHERE l.consultaId IS NOT NULL 
    #                     and l.clientId IS NOT NULL;"""
    #     return query
    
    def get_crm_autoatendimento_postgres_aws(self):
        query = f"""SELECT 
                        l."simulacaoCltId" as "consultaId", 
                        l."clienteId",
                        l.telefone AS telefone_lead
                    FROM public."AutoAtendimento" l
                    WHERE l."simulacaoCltId" IS NOT null
                        and l."clienteId" IS NOT NULL;"""
        return query
    
    # def get_crm_tabela_mysql_aws(self):
    #     query = f"""SELECT 
    #                     tb.id AS tabelaId,
    #                     tb.nome AS tabela
    #                 FROM CRM.Tabelas tb;"""
    #     return query
    
    def get_crm_tabela_postgres_aws(self):
        query = f"""SELECT 
                        CAST(tb.id AS integer) AS "tabelaId",
                        tb.nome AS tabela
                    FROM public."Tabelas" tb;"""
        return query
    
    # def get_crm_parcela_mysql_aws(self):
    #     query = f"""SELECT 
    #                     pc.consultaId,
    #                     pc.num AS parcelas
    #                 FROM CRM.Parcelas pc;"""
    #     return query
    
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
        # query_corban = f"""
        #                 select
        #                     c.nome,
        #                     LPAD(REGEXP_REPLACE(c.cpf, '\D', '', 'g')::text, 11, '0') as cpf_corban,
        #                     CASE
        #                         WHEN LENGTH(t.telefone) < 11
        #                         THEN SUBSTRING(t.telefone FROM 1 FOR 2) || '9' || SUBSTRING(t.telefone FROM 3)
        #                         ELSE t.telefone
        #                     END AS numero_corban,
        #                     dt.pagamento as liberacao,
        #                     ct.valor_financiado,
        #                     ct.valor_liberado,
        #                     ct.valor_parcela,
        #                     ct.prazo,
        #                     ct.banco_nome as nome_banco,
        #                     cc.valor as valor_comissao,
        #                     ct.tabela_id
        #                 from unificados.clientes c 
        #                 left join unificados.telefones t 
        #                     on c.id = t.cliente_id
        #                 left join unificados.contrato ct
        #                     on c.id = ct.cliente_id 
        #                 left join unificados.propostas p
        #                     on ct.proposta_id_corban = cast(p.proposta_id as varchar)
        #                 left join unificados.api a
        #                     on ct.proposta_id_corban = a.proposta_id_corban
        #                 left join unificados.comissoes cc
        #                     on ct.proposta_id_corban = cc.proposta_id_corban
        #                 left join unificados.datas dt
        #                     on ct.proposta_id_corban = dt.proposta_id_corban
        #                 where (a.status_api in ('APROVADA')
        #                     or (p.status_nome = 'Pago' and ct.banco_nome = 'Credspot'));
        #                 """
        query_corban = """
                        select
                            c.nome,
                            LPAD(REGEXP_REPLACE(c.cpf, '\D', '', 'g')::text, 11, '0') as cpf_corban,
                            dt.pagamento as liberacao,
                            ct.valor_financiado,
                            ct.valor_liberado,
                            ct.valor_parcela,
                            ct.prazo,
                            ct.banco_nome as nome_banco,
                            ct.proposta_id_corban as proposta_id,
                            ct.tabela_id
                        from unificados.propostas p
                        left join unificados.contrato ct
                            on p.proposta_id = cast(ct.proposta_id_corban  as integer)
                        left join unificados.api a
                            on ct.proposta_id_corban = a.proposta_id_corban
                        left join unificados.datas dt
                            on ct.proposta_id_corban = dt.proposta_id_corban
                        left join unificados.clientes c
                            on ct.cliente_id_corban = c.cliente_id
                        where (a.status_api in ('APROVADA')
                            or (p.status_nome = 'Pago' and ct.banco_nome = 'Credspot'));
                        """
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
                        p."valorTotalComissao",
                        t.codigo
                    from public."AutoAtendimento" aa 
                    left join public."Consultas" cs 
                        on aa."simulacaoFgtsId" = cs.id or aa."simulacaoCltId" = cs.id
                    left join public."Propostas" p 
                        on cs.id = p."consultaId" 
                    left join public."Tabelas" t 
                        on cs."tabelaId" = t.id 
                    left join public."Bancos" b 
                        on cs."bancoId" = b.id
                    left join public."Clientes" c 
                        on aa."clienteId" = c.id;
                """
        return query_digisac, query_corban, query_crm
        # return query
        
    def get_telefones(self):
        query_crm = """
                select distinct
                    c.cpf as cpf_corban, 
                    CASE
                        WHEN LENGTH(t.numero) < 11
                        THEN SUBSTRING(t.numero FROM 1 FOR 2) || '9' || SUBSTRING(t.numero FROM 3)
                        ELSE t.numero
                    END AS numero_corban
                from public."Clientes" c 
                left join public."Telefones" t 
                    on c.id = t."clienteId";
                """
        query_corban = """
                        select 
                            LPAD(REGEXP_REPLACE(c.cpf, '\D', '', 'g')::text, 11, '0') as cpf_corban,
                            CASE
                                WHEN LENGTH(t.telefone) < 11
                                THEN SUBSTRING(t.telefone FROM 1 FOR 2) || '9' || SUBSTRING(t.telefone FROM 3)
                                ELSE t.telefone
                            END AS numero_corban
                        from unificados.clientes c 
                        left join unificados.telefones t 
                            on c.cliente_id = t.cliente_id_corban
                        where LPAD(REGEXP_REPLACE(c.cpf, '\D', '', 'g')::text, 11, '0') = ANY(%s);
                        """
        return query_crm, query_corban
    
    def get_campanhas_meta(self):
        query = """
                SELECT * FROM controle.campanhas;
                """
        return query
    
    def get_comissoes_corban(self):
        query = """
                select 
                    cc.proposta_id_corban as proposta_id,
                    sum(cc.valor) as valor_comissao
                from unificados.comissoes cc
                group by cc.proposta_id_corban, cc.valor;
                """
        return query
    
    def get_tabelas_comissao(self):
        query = """
                select
                    cast(t.tabela_codigo as varchar) as codigo,
                    t.percentual_valor_liberado as percentual,
                    case 
                        when t.prazo_inicio is null then 0
                        else t.prazo_inicio
                    end as prazo_inicio,
                    case
                        when t.prazo_fim is null then 99
                        else t.prazo_fim
                    end as prazo_fim 
                from unificados.tabelas t
                where t.vigencia_fim is null
                    and t.percentual_valor_liberado > 0;
                """
        return query

    #####################################################################
    def get_campanhas_teste(self):
        query_corban = f"""
                        select
                            c.nome,
                            LPAD(REGEXP_REPLACE(c.cpf, '\D', '', 'g')::text, 11, '0') as cpf_corban,
                            CASE
                                WHEN LENGTH(t.telefone) < 11
                                THEN SUBSTRING(t.telefone FROM 1 FOR 2) || '9' || SUBSTRING(t.telefone FROM 3)
                                ELSE t.telefone
                            END AS numero_corban,
                            dt.pagamento as liberacao,
                            ct.valor_financiado,
                            ct.valor_liberado,
                            ct.valor_parcela,
                            ct.prazo,
                            ct.banco_nome as nome_banco,
                            cc.valor as valor_comissao
                        from unificados.clientes c 
                        left join unificados.telefones t 
                            on c.id = t.cliente_id
                        left join unificados.contrato ct
                            on c.id = ct.cliente_id 
                        left join unificados.propostas p
                            on ct.proposta_id_corban = cast(p.proposta_id as varchar)
                        left join unificados.api a
                            on ct.proposta_id_corban = a.proposta_id_corban
                        left join unificados.comissoes cc
                            on ct.proposta_id_corban = cc.proposta_id_corban
                        left join unificados.datas dt
                            on ct.proposta_id_corban = dt.proposta_id_corban
                        where 
                            dt.cadastro >= '2026-01-05' 
                            and (a.status_api in ('APROVADA')
                            or (p.status_nome = 'Pago' and ct.banco_nome = 'Credspot'));
                        """
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
                    from public."AutoAtendimento" aa
                    left join public."Propostas" p 
                        on p."clienteId" = aa."clienteId"
                    left join public."Clientes" c 
                        on aa."clienteId" = c.id
                    left join public."Consultas" cs
                        on p."consultaId" = cs.id
                    left join public."Bancos" b 
                        on cs."bancoId" = b.id;
                """
                    # where aa."createdAt" >= '2026-01-05T00:00:00';
        return query_corban, query_crm
    
    def get_teste_corban_propostas(self):
        query = f"""
                    select
                        c.nome,
                        LPAD(REGEXP_REPLACE(c.cpf, '\D', '', 'g')::text, 11, '0') as cpf_corban,
                        CASE
                            WHEN LENGTH(t.telefone) < 11
                            THEN SUBSTRING(t.telefone FROM 1 FOR 2) || '9' || SUBSTRING(t.telefone FROM 3)
                            ELSE t.telefone
                        END AS numero_corban,
                        dt.pagamento as liberacao,
                        ct.valor_financiado,
                        ct.valor_liberado,
                        ct.valor_parcela,
                        ct.prazo,
                        ct.banco_nome as nome_banco,
                        cc.valor as valor_comissao
                    from unificados.clientes c 
                    left join unificados.telefones t 
                        on c.id = t.cliente_id
                    left join unificados.contrato ct
                        on c.id = ct.cliente_id 
                    left join unificados.propostas p
                        on ct.proposta_id_corban = cast(p.proposta_id as varchar)
                    left join unificados.api a
                        on ct.proposta_id_corban = a.proposta_id_corban
                    left join unificados.comissoes cc
                        on ct.proposta_id_corban = cc.proposta_id_corban
                    left join unificados.datas dt
                        on ct.proposta_id_corban = dt.proposta_id_corban
                    where 
                        dt.cadastro >= '2026-01-05' 
                        and (a.status_api in ('APROVADA')
                        or (p.status_nome = 'Pago' and ct.banco_nome = 'Credspot'));
                    """
        return query

    def get_teste_crm_autoatendimento(self):
        query = f"""
                    select 
                        aa."clienteId",
                        aa.telefone as numero,
                        aa."createdAt",
                        aa."mensagemInicial"
                    from public."AutoAtendimento" aa
                    where aa."createdAt" >= '2026-01-05T00:00:00';
                """
        return query
    
    def get_teste_crm_propostas(self):
        query = f"""
                    select 
                        p."clienteId",
                        p."consultaId",
                        p."dataPagamento",
                        p."valorLiberado",
                        p.prazo,
                        p."valorTotalComissao"
                    from public."Propostas" p;
                """
        return query
    
    def get_teste_crm_clientes(self):
        query = f"""
                    select 
                        c.id,
                        c.cpf,
                        c.nome
                    from public."Clientes" c;
                """
        return query
    
    def get_teste_crm_consultas(self):
        query = f"""
                    select 
                        cs.id,
                        cs."bancoId",
                        cs."valorBruto"
                    from public."Consultas" cs;
                """
        return query
    
    def get_teste_crm_bancos(self):
        query = f"""
                    select 
                        b.id,
                        b.nome as nome_banco
                    from public."Bancos" b;
                """
        return query



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