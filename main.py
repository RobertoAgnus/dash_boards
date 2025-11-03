import streamlit as st

# Inicializa a navegação
main_page = st.Page("clientes_atendidos.py" , title="Clientes Atendidos" )
page_2    = st.Page("clientes_novos.py"     , title="Clientes Novos"     )
page_3    = st.Page("disparos_realizados.py", title="Disparos Realizados")
page_4    = st.Page("base_fgts.py"          , title="Base FGTS -> CLT"   )
page_5    = st.Page("comissoes_corban.py"   , title="Comissões Corban"   )
page_6    = st.Page("base_fgts_v1.py"  , title="TESTE")

# Configurar a navegação
pg = st.navigation([main_page, page_2, page_3, page_4, page_5, page_6])

# Executar a página selecionada
pg.run()

