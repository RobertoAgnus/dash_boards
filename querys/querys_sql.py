class QuerysSQL:
    def __init__(self):
        ...

    ##### CLIENTES ATENDIDOS #####
    def etapa_atendimentos(self):
        query = '''select distinct
                        l.etapa as Etapa
                    from CRM.Leads l
                    order by l.etapa;'''
        
        return query
    
    def datas_atendimentos(self):
        query = f"""select distinct 
                        date_format(CAST(l.data AS DATE), '%d/%m/%Y') as data 
                    from CRM.Leads l
                    order by l.data asc;"""
        
        return query
    
    def total_clientes(self):
        query = f"""select 
                        count(distinct c.CPF) as TOTAL_CPF
                    from CRM.Clientes c;"""
        
        return query
    
    def clientes_atendidos(self, status=None, data=None):
        condicao = None
        if status == 'Selecionar' and data != 'Selecionar':
            condicao = f"""where l.data {data}"""
        elif data == 'Selecionar' and status != 'Selecionar':
            condicao = f"""where l.etapa='{status}'"""
        elif data != 'Selecionar' and status != 'Selecionar':
            condicao = f"""where l.etapa='{status}'
                            and l.data {data}"""
        elif status == 'Selecionar' and data == 'Selecionar':
            condicao = "where 1 = 1"

        query = f"""select distinct
                        date_format(l.data, '%d/%m/%Y') as Data,
                        c.CPF as CPF, 
                        c.nome as Nome, 
                        l.telefone as telefoneLead,
                        e.cidade as Cidade, 
                        e.estado as UF, 
                        l.etapa as Etapa
                    from CRM.Clientes c 
                    right join CRM.Leads l on c.id = l.clientId
                    left join CRM.Enderecos e on c.id = e.clientId
                    {condicao}
                    order by l.data desc;"""
        
        return query
    
    def clientes_atendidos_v1(self):
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
    def total_disparos(self, data):
        condicao = None
        if data != 'Selecionar':
            condicao = f'where data {data}'
        else:
            'where 1 = 1'

        query = f"""select 
                        count(*) as total
                    from "disparos".disparos
                    {condicao};"""
        return query
    
    def status_disparos(self):
        query = """select distinct 
                        status 
                    from "disparos".disparos;"""
        return query
    
    def datas_disparos(self):
        query = """select distinct 
                        data 
                    from "disparos".disparos 
                    order by data asc;"""
        return query
    
    def contagem_de_disparos(self, status=None, data=None):
        condicao = None
        if status == 'Selecionar' and data != 'Selecionar':
            condicao = f"""where data {data}"""
        elif data == 'Selecionar' and status != 'Selecionar':
            condicao = f"""where status = '{status}'"""
        elif data != 'Selecionar' and status != 'Selecionar':
            condicao = f"""where status = '{status}'
                            and data {data}"""
        elif status == 'Selecionar' and data == 'Selecionar':
            condicao = "where 1 = 1"

        query = f"""with consulta as (
                        select 
                            "CPF", 
                            telefone,
                            data
                        from "disparos".disparos 
                        {condicao}
                    )
                    SELECT
                        COUNT(*) AS "TOTAL"
                    FROM
                        consulta
                    GROUP BY
                        data,
                        telefone,
                        "CPF";"""
        return query
    
    def disparos_por_cliente(self, status=None, data=None, disparos=None):
        condicao = None
        if status == 'Selecionar' and data != 'Selecionar':
            condicao = f"""where data {data}"""
        elif data == 'Selecionar' and status != 'Selecionar':
            condicao = f"""where status = '{status}'"""
        elif data != 'Selecionar' and status != 'Selecionar':
            condicao = f"""where status = '{status}'
                            and data {data}"""
        elif status == 'Selecionar' and data == 'Selecionar':
            condicao = "where 1 = 1"

        query = f"""select 
                        to_char(data, 'DD/MM/YYYY') as data,
                        "CPF", 
                        telefone, 
                        valor, 
                        status, 
                        vendedor
                    from "disparos".disparos 
                    {condicao} 
                    order by data desc;""" 
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

    def tabela_corban(self, origem, data):
        condicao = None
        if origem == 'Selecionar':
            condicao = f"""where cc.data {data}"""
        elif origem != 'Selecionar':
            condicao = f"""where pc.origem='{origem}'
                            and cc.data {data}"""
        
        # query = f"""select
        #                 p.data_status as "Data Status", 
        #                 p.origem as "Origem", 
        #                 p.status_nome as "Status", 
        #                 c.valor as "Valor", 
        #                 c.data as "Data da Comissão", 
        #                 co.percentual_valor_base as "Percetual Valor Base", 
        #                 co.valor_fixo as "Valor Fixo"
        #             from "propostasCorban".propostas_concatenado p
        #             left join "propostasCorban".comissoes_concatenado c
        #             on p.proposta_id = c.proposta_id
        #             left join "propostasCorban".comissionamentos_concatenado co
        #             on p.proposta_id = co.proposta_id
        #             {condicao}
        #             order by p.data_status;"""
        query = f"""select 
                        cc.data as "Data da Comissão",
                        pc.origem as "Origem",
                        cc.valor as "Valor",
                        pc.proposta_id as "Proposta"
                    from "propostasCorban".comissoes_concatenado cc 
                    left join "propostasCorban".propostas_concatenado pc
                        on cc.proposta_id = pc.proposta_id
                    {condicao} 
                    order by cc.data asc;"""
        return query
    
    def qtd_comissoes_total(self, origem, data):
        condicao = None
        if origem == 'Selecionar':
            condicao = f"""where cc.data {data}"""
        elif origem != 'Selecionar':
            condicao = f"""where pc.origem='{origem}'
                            and cc.data {data}"""
            
        query = f"""select 
                        sum(cc.valor) as total
                    from "propostasCorban".comissoes_concatenado cc 
                    left join "propostasCorban".propostas_concatenado pc
                        on cc.proposta_id = pc.proposta_id 
                    {condicao};"""
        return query
    
    def qtd_comissoes_pagas(self, origem, data):
        condicao = None
        if origem == 'Selecionar':
            condicao = f"""and TO_TIMESTAMP(ac.data_status_api, 'YYYY-MM-DD HH24:MI:SS')::date {data}"""
        elif origem != 'Selecionar':
            condicao = f"""and psc.origem='{origem}' 
                            and TO_TIMESTAMP(ac.data_status_api, 'YYYY-MM-DD HH24:MI:SS')::date {data}"""
        
        query = f"""select 
                        sum(cc.valor) as pago
                    from "propostasCorban".api_concatenado ac 
                    left join "propostasCorban".comissoes_concatenado cc 
                        on ac.proposta_id = cc.proposta_id 
                    left join "propostasCorban".datas_concatenado dc 
                        on ac.proposta_id = dc.proposta_id 
                    left join "propostasCorban".proposta_concatenado pc 
                        on ac.proposta_id = pc.proposta_id 
                    left join "propostasCorban".propostas_concatenado psc 
                        on ac.proposta_id = psc.proposta_id
                    where cc.valor is not null
                        and dc.cancelado is null
                        and dc.pagamento is not null
                        {condicao}
                        and ac.status_api = 'APROVADA'
                        and pc.valor_total_comissionado <> '0'
                        and psc.status_nome <> 'Cancelado';"""
        return query
    
    def qtd_comissoes_aguardando(self, origem, data):
        condicao = None
        if origem == 'Selecionar':
            condicao = f"""and TO_TIMESTAMP(ac.data_status_api, 'YYYY-MM-DD HH24:MI:SS')::date {data}"""
        elif origem != 'Selecionar':
            condicao = f"""and psc.origem='{origem}' 
                            and TO_TIMESTAMP(ac.data_status_api, 'YYYY-MM-DD HH24:MI:SS')::date {data}"""
        
        query = f"""select 
                        sum(CASE
                                WHEN cc2.recebe_valor_base ~ '^[0-9,.]+$' THEN
                                    REPLACE(cc2.recebe_valor_base, ',', '.')::NUMERIC
                                ELSE 0
                            end
                        ) as aguardando
                    from "propostasCorban".api_concatenado ac 
                    left join "propostasCorban".comissoes_concatenado cc 
                        on ac.proposta_id = cc.proposta_id 
                    left join "propostasCorban".comissionamentos_concatenado cc2 
                        on ac.proposta_id = cc2.proposta_id 
                    left join "propostasCorban".datas_concatenado dc 
                        on ac.proposta_id = dc.proposta_id 
                    left join "propostasCorban".proposta_concatenado pc 
                        on ac.proposta_id = pc.proposta_id 
                    left join "propostasCorban".propostas_concatenado psc 
                        on ac.proposta_id = psc.proposta_id
                    where cc.valor is null
                        and dc.cancelado is null
                        {condicao}
                        and cc2.recebe_valor_base <> '0.0'
                        and cc2.recebe_valor_base <> 'NaN'
                        and ac.status_api = 'APROVADA'
                        and pc.valor_total_comissionado = '0'
	                    and psc.status_nome <> 'Cancelado';"""
        return query
    
    ##### POSTGRESQL #####
    # def data_proposta_corban(self):
    #     query = f"""select
    #                     p.data_status as data
    #                 from "propostasCorban".propostas_concatenado p;"""
    #     return query
    
    # def origem_proposta_corban(self):
    #     query = f"""select distinct
    #                     p.origem as origem
    #                 from "propostasCorban".propostas_concatenado p
    #                 order by p.origem;"""
    #     return query

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
