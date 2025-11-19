import streamlit as st
import pandas as pd
import bcrypt

from querys.connect import Conexao
from psycopg2.extras import execute_values


if "authenticated" not in st.session_state:
    st.error("Acesso negado. Faça login.")
    st.stop()

# verifica master
if not st.session_state.master:  
    st.error("Acesso restrito ao usuário master.")
    st.stop()

# Inicializa os campos
if "cad_user" not in st.session_state:
    st.session_state.cad_user = ""
if "cad_pwd" not in st.session_state:
    st.session_state.cad_pwd = ""
if "cad_rpwd" not in st.session_state:
    st.session_state.cad_rpwd = ""
if "cad_check" not in st.session_state:
    st.session_state.cad_check = False

if "alt_user" not in st.session_state:
    st.session_state.alt_user = ""
if "alt_pwd" not in st.session_state:
    st.session_state.alt_pwd = ""
if "alt_rpwd" not in st.session_state:
    st.session_state.alt_rpwd = ""
if "alt_check" not in st.session_state:
    st.session_state.alt_check = False

if "exc_user" not in st.session_state:
    st.session_state.exc_user = ""

def executa_crud(flag, df):
    conectar = Conexao()

    conectar.conectar_postgres()
    conn = conectar.obter_conexao_postgres()

    cur = conn.cursor()

    if flag == 'inserir':
        query = 'INSERT INTO controle.usuarios (usuario, senha, master) values %s;'
        
        valores = [tuple(x) for x in df.to_numpy()]

        execute_values(cur, query, valores)
        
        conn.commit()
        cur.close()

        retorno = 'Usuário cadastrado com sucesso.'

    elif flag == 'alterar':
        dados = (df['senha'].iloc[0], bool(df['master'].iloc[0]), df['usuario'].iloc[0])
        
        query = '''UPDATE controle.usuarios
                    SET 
                        senha = %s,
                        master = %s
                    WHERE usuario = %s;'''
        
        cur.execute(query, dados)
        
        conn.commit()
        cur.close()

        retorno = 'Senha alterada com sucesso.'

    elif flag == 'excluir':
        usuario = df['usuario']

        query = '''DELETE FROM controle.usuarios
                    WHERE usuario = %s;'''
        
        cur.execute(query, usuario)
        
        conn.commit()
        cur.close()

        retorno = 'Usuário excluído com sucesso.'

    conectar.desconectar_postgres()
    
    return retorno

def cadastrar_usuario():
    hashed = bcrypt.hashpw(new_pass.encode(), bcrypt.gensalt()).decode()
    df = pd.DataFrame({'usuario': [new_user], 'senha': [hashed], 'master': [check]})

    retorno = executa_crud('inserir', df)

    st.session_state.cad_user = ""
    st.session_state.cad_pwd = ""
    st.session_state.cad_rpwd = ""
    st.session_state.cad_check = False

    st.code(retorno)

def alterar_senha():
    hashed = bcrypt.hashpw(new_pass.encode(), bcrypt.gensalt()).decode()
    df = pd.DataFrame({'usuario': [user], 'senha': [hashed], 'master': [check]})

    retorno = executa_crud('alterar', df)

    st.session_state.alt_user = ""
    st.session_state.alt_pwd = ""
    st.session_state.alt_rpwd = ""
    st.session_state.alt_check = False

    st.code(retorno)

def excluir_usuario():
    df = pd.DataFrame({'usuario': [user]})

    retorno = executa_crud('excluir', df)

    st.session_state.exc_user = ""

    st.code(retorno)
    

st.title("👑 Gerenciamento de Usuários")
st.write("Apenas o usuário MASTER pode ver esta página.")

with st.sidebar:
    st.title('Cadastro')

    ##### FILTRO DE CRUD #####
    if "selecao_cadastro" not in st.session_state:
        st.session_state.selecao_cadastro = "Cadastrar novo Usuário"

    lista_opcao = ["Cadastrar novo Usuário", "Alterar Senha", "Excluir Usuário"]

    seleciona_crud = st.radio(
        'Selecione Opção',
        lista_opcao,
        key='selecao_cadastro'
    )

if seleciona_crud == 'Cadastrar novo Usuário':
    new_user = st.text_input("Novo usuário", key='cad_user')
    new_pass = st.text_input("Senha", type="password", key='cad_pwd')
    repete_pass = st.text_input("Repita Senha", type="password", key='cad_rpwd')
    check = st.checkbox('É usuário MASTER?', key='cad_check')

    habilita = True
    if new_pass == repete_pass and (new_pass != '' or repete_pass != '') and new_user != '':
            habilita = False
    elif new_pass != repete_pass:
        st.code('Senhas diferentes.')
        
    st.button("Cadastrar novo Usuário", disabled=habilita, on_click=cadastrar_usuario)
        

elif seleciona_crud == 'Alterar Senha':
    user = st.text_input("Usuário", key='alt_user')
    new_pass = st.text_input("Nova Senha", type="password", key='alt_pwd')
    repete_pass = st.text_input("Repita Nova Senha", type="password", key='alt_rpwd')
    check = st.checkbox('É usuário MASTER?', key='alt_check')

    habilita = True
    if new_pass == repete_pass and (new_pass != '' or repete_pass != '') and user != '':
            habilita = False
    elif new_pass != repete_pass:
        st.code('Senhas diferentes.')

    st.button("Alterar Senha", disabled=habilita, on_click=alterar_senha)
        

elif seleciona_crud == 'Excluir Usuário':
    user = st.text_input("Usuário a ser excluído", key='exc_user')
    
    st.button("Excluir Usuário", on_click=excluir_usuario)
