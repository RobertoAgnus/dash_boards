import streamlit as st
import altair as alt
import pandas as pd
import bcrypt
import json

from querys.connect import Conexao

##### CONFIGURAÇÃO DA PÁGINA #####
alt.themes.enable("dark")

def obter_usuario(usuario):
    conectar = Conexao()

    conectar.conectar_postgres()
    conn = conectar.obter_conexao_postgres()

    query = f"SELECT * FROM controle.usuarios WHERE usuario = '{usuario}';"

    df = pd.read_sql_query(query, conn)

    lista_json = df.to_dict(orient="records")

    conectar.desconectar_postgres()
    
    return lista_json

# ---------- Configurações (use st.secrets para produção) ----------
USERS_HASHES = st.secrets.get("auth", {}).get("users", {})

# ---------- Inicialização do estado ----------
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if "username" not in st.session_state:
    st.session_state.username = None


# ---------- Funções utilitárias ----------
def verify_password(password: str, hashed: str) -> bool:
    """Verifica senha com bcrypt."""
    try:
        return bcrypt.checkpw(password.encode(), hashed.encode())
    except Exception:
        return False

def login_user(username: str):
    st.session_state.authenticated = True
    st.session_state.username = username[0]['usuario']
    st.session_state.master = username[0]['master']

def logout_user():
    st.session_state.authenticated = False
    st.session_state.username = None


# ---------- Interface de login ----------
def show_login():
    st.title("Controle de Leads")
    st.markdown("### 🔒 Login")

    st.set_page_config(
        page_title="Login", 
        layout="centered", 
        page_icon="image/logo_agnus.ico",
        initial_sidebar_state="expanded"
    )

    with st.form("login_form"):
        username  = st.text_input("Usuário")
        password  = st.text_input("Senha", type="password")
        submitted = st.form_submit_button("Entrar")

    df = obter_usuario(username)
    
    if submitted:
        if len(df) == 1:
            if verify_password(password, df[0]['senha']):
                login_user(df)
                st.rerun()
            else:
                st.error("Senha inválida.")
        else:
            st.error("Usuário inválido.")

def navegacao():
    # Páginas
    main_page     = st.Page("clientes_atendidos.py" , title="Clientes Atendidos"     )
    page_2        = st.Page("clientes_novos.py"     , title="Clientes Novos"         )
    page_3        = st.Page("disparos_realizados.py", title="Disparos Realizados"    )
    page_4        = st.Page("base_fgts.py"          , title="Base FGTS -> CLT"       )
    page_5        = st.Page("comissoes_corban.py"   , title="Comissões Corban"       )
    page_6        = st.Page("campanhas.py"          , title="Campanhas Publicitárias")
    page_master   = st.Page("gerenciar_usuario.py"  , title="Gerenciar Usuário"      )
    page_user     = st.Page("alterar_senha.py"      , title="Alterar Senha"          )

    # Nav do usuário master
    if st.session_state.master:
        pg = st.navigation([
            # main_page,
            page_6,
            page_master,
        ])
    else:
        pg = st.navigation([
            # main_page,
            page_user
        ])

    pg.run()

# ---------- Página protegida ----------
def show_protected_page():
    st.sidebar.write(f"Logado como: **{st.session_state.username}**")

    if st.sidebar.button("Sair"):
        logout_user()
        st.rerun()

    navegacao()

# ---------- Fluxo principal ----------
def main():
    if st.session_state.authenticated:
        show_protected_page()
    else:
        show_login()

if __name__ == "__main__":
    main()


# import streamlit as st
# import altair as alt
# import pandas as pd
# import bcrypt

# from streamlit_cookies_manager import EncryptedCookieManager
# from querys.connect import Conexao

# ##### CONFIGURAÇÃO DA PÁGINA #####
# alt.themes.enable("dark")

# st.set_page_config(
#     page_title="Controle de Leads",
#     layout="centered",
#     page_icon="image/logo_agnus.ico",
# )

# ##### COOKIES #####
# cookies = EncryptedCookieManager(
#     prefix="controle_",
#     password=st.secrets["cookie_secret"]
# )

# if not cookies.ready():
#     st.stop()

# ##### FUNÇÕES DE BANCO #####
# def obter_usuario(usuario):
#     conectar = Conexao()
#     conectar.conectar_postgres()
#     conn = conectar.obter_conexao_postgres()

#     query = "SELECT usuario, senha, master FROM controle.usuarios WHERE usuario = %s;"
#     df = pd.read_sql_query(query, conn, params=(usuario,))

#     conectar.desconectar_postgres()
#     return df.to_dict(orient="records")

# ##### RESTAURA SESSÃO A PARTIR DO COOKIE #####
# if "authenticated" not in st.session_state:
#     st.session_state.authenticated = cookies.get("authenticated") == "true"

# if "username" not in st.session_state:
#     st.session_state.username = cookies.get("username")

# if "master" not in st.session_state:
#     st.session_state.master = cookies.get("master") == "true"

# ##### FUNÇÕES UTILITÁRIAS #####
# def verify_password(password: str, hashed: str) -> bool:
#     try:
#         return bcrypt.checkpw(password.encode(), hashed.encode())
#     except Exception:
#         return False

# def login_user(user_data):
#     st.session_state.authenticated = True
#     st.session_state.username = user_data["usuario"]
#     st.session_state.master = bool(user_data["master"])

#     cookies["authenticated"] = "true"
#     cookies["username"] = user_data["usuario"]
#     cookies["master"] = str(bool(user_data["master"])).lower()
#     cookies.save()

# def logout_user():
#     cookies.clear()
#     st.session_state.clear()

# ##### LOGIN #####
# def show_login():
#     st.title("Controle de Leads")
#     st.markdown("### 🔒 Login")

#     with st.form("login_form"):
#         username = st.text_input("Usuário")
#         password = st.text_input("Senha", type="password")
#         submitted = st.form_submit_button("Entrar")

#     if submitted:
#         dados = obter_usuario(username)

#         if len(dados) == 1 and verify_password(password, dados[0]["senha"]):
#             login_user(dados[0])
#             st.rerun()
#         else:
#             st.error("Usuário ou senha inválidos.")

# ##### NAVEGAÇÃO #####
# def navegacao():
#     main_page   = st.Page("clientes_atendidos.py", title="Clientes Atendidos")
#     page_6      = st.Page("campanhas.py", title="Campanhas Publicitárias")
#     page_master = st.Page("gerenciar_usuario.py", title="Gerenciar Usuário")
#     page_user   = st.Page("alterar_senha.py", title="Alterar Senha")

#     if st.session_state.master:
#         pg = st.navigation([main_page, page_6, page_master])
#     else:
#         pg = st.navigation([main_page, page_user])

#     pg.run()

# ##### PÁGINA PROTEGIDA #####
# def show_protected_page():
#     with st.sidebar:
#         st.write(f"Logado como: **{st.session_state.username}**")
#         if st.button("Sair"):
#             logout_user()
#             st.rerun()

#     navegacao()

# ##### FLUXO PRINCIPAL #####
# def main():
#     if st.session_state.authenticated:
#         show_protected_page()
#     else:
#         show_login()

# if __name__ == "__main__":
#     main()

# import streamlit as st
# import altair as alt
# import pandas as pd
# import bcrypt

# from streamlit_cookies_manager import EncryptedCookieManager
# from querys.connect import Conexao

# ##### CONFIGURAÇÃO VISUAL #####
# alt.themes.enable("dark")

# st.set_page_config(
#     page_title="Controle de Leads",
#     layout="wide",
#     page_icon="image/logo_agnus.ico",
#     initial_sidebar_state="collapsed"
# )

# # ---------- COOKIES ----------
# cookies = EncryptedCookieManager(
#     prefix="auth_",
#     password="CHAVE_FORTE_E_SECRETA_AQUI"
# )

# if not cookies.ready():
#     st.stop()

# # ---------- BANCO ----------
# def obter_usuario(usuario):
#     conectar = Conexao()
#     conectar.conectar_postgres()
#     conn = conectar.obter_conexao_postgres()

#     query = "SELECT usuario, senha, master FROM controle.usuarios WHERE usuario = %s;"
#     df = pd.read_sql_query(query, conn, params=[usuario])

#     conectar.desconectar_postgres()
#     return df.to_dict(orient="records")

# # ---------- SESSION STATE (RESTORE) ----------
# if "authenticated" not in st.session_state:
#     st.session_state.authenticated = cookies.get("authenticated") == "true"

# if "username" not in st.session_state:
#     st.session_state.username = cookies.get("username")

# if "master" not in st.session_state:
#     st.session_state.master = cookies.get("master") == "True"

# # ---------- AUTENTICAÇÃO ----------
# def verify_password(password: str, hashed: str) -> bool:
#     try:
#         return bcrypt.checkpw(password.encode(), hashed.encode())
#     except Exception:
#         return False

# def login_user(user_row):
#     st.session_state.authenticated = True
#     st.session_state.username = user_row["usuario"]
#     st.session_state.master = user_row["master"]

#     cookies["authenticated"] = "true"
#     cookies["username"] = user_row["usuario"]
#     cookies["master"] = str(user_row["master"])
#     cookies.save()

# def logout_user():
#     cookies.clear()
#     st.session_state.clear()

# # ---------- LOGIN ----------
# def show_login():
#     st.title("Controle de Leads")
#     st.markdown("### 🔒 Login")

#     with st.form("login_form"):
#         username = st.text_input("Usuário")
#         password = st.text_input("Senha", type="password")
#         submitted = st.form_submit_button("Entrar")

#     if submitted:
#         df = obter_usuario(username)

#         if len(df) == 1 and verify_password(password, df[0]["senha"]):
#             login_user(df[0])
#             st.rerun()
#         else:
#             st.error("Usuário ou senha inválidos.")

# # ---------- NAVEGAÇÃO ----------
# def navegacao():
#     main_page   = st.Page("clientes_atendidos.py", title="Clientes Atendidos")
#     page_6      = st.Page("campanhas.py", title="Campanhas Publicitárias")
#     page_master = st.Page("gerenciar_usuario.py", title="Gerenciar Usuário")
#     page_user   = st.Page("alterar_senha.py", title="Alterar Senha")

#     if st.session_state.master:
#         pg = st.navigation([main_page, page_6, page_master])
#     else:
#         pg = st.navigation([main_page, page_user])

#     pg.run()

# # ---------- PÁGINA PROTEGIDA ----------
# def show_protected_page():
#     with st.sidebar:
#         st.write(f"Logado como: **{st.session_state.username}**")
#         if st.button("Sair"):
#             logout_user()
#             st.rerun()

#     navegacao()

# # ---------- FLUXO PRINCIPAL ----------
# def main():
#     if st.session_state.authenticated:
#         show_protected_page()
#     else:
#         show_login()

# if __name__ == "__main__":
#     main()
