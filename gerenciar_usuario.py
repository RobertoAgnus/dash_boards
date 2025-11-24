import streamlit as st
import pandas as pd
import bcrypt

from datetime import datetime
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

class ExecutaCrud:
    def __init__(self):
        self.conectar = Conexao()

        self.conectar.conectar_postgres()
        self.conn = self.conectar.obter_conexao_postgres()

        self.cur = self.conn.cursor()

    def desconectar(self):
        self.conectar.desconectar_postgres()

    # if flag == 'inserir':
    def inserir(self, df):
        query = 'INSERT INTO controle.usuarios (usuario, senha, master, responsavel, criado_em) values %s;'
        
        valores = [tuple(x) for x in df.to_numpy()]

        execute_values(self.cur, query, valores)
        
        self.conn.commit()
        self.cur.close()

        self.desconectar()

        return 'Usuário cadastrado com sucesso.'

    # elif flag == 'alterar':
    def alterar(self, df):
        data = datetime.now()
        dados = (df['senha'].iloc[0], bool(df['master'].iloc[0]), df['responsavel'].iloc[0], data, df['usuario'].iloc[0])
        
        query = '''UPDATE controle.usuarios
                    SET 
                        senha = %s,
                        master = %s,
                        responsavel = %s,
                        atualizado_em = %s
                    WHERE usuario = %s;'''
        
        self.cur.execute(query, dados)
        
        self.conn.commit()
        self.cur.close()

        self.desconectar()

        return 'Senha alterada com sucesso.'

    # elif flag == 'excluir':
    def excluir(self, df):
        usuario = (df['usuario'].iloc[0],)
        dados = (df['responsavel'].iloc[0], datetime.now(), df['usuario'].iloc[0])
        
        query = '''INSERT INTO controle.excluidos (usuario, senha, master, responsavel_criacao, responsavel_exclusao, criado_em, atualizado_em)
                    SELECT usuario, senha, master, responsavel, %s, criado_em, %s
                    FROM controle.usuarios
                    WHERE usuario = %s;'''

        self.cur.execute(query, dados)

        self.conn.commit()

        query = '''DELETE FROM controle.usuarios
                    WHERE usuario = %s;'''
        
        self.cur.execute(query, usuario)
        
        self.conn.commit()
        self.cur.close()

        self.desconectar()

        return 'Usuário excluído com sucesso.'

    # elif flag == 'consultar':
    def consultar(self):
        query = 'select usuario, master, responsavel, criado_em, atualizado_em from controle.usuarios;'

        df = pd.read_sql_query(query, self.conn)

        self.desconectar()

        return df
    
    def consultar_excluidos(self):
        query = 'select usuario, master, responsavel_criacao, responsavel_exclusao, criado_em, atualizado_em from controle.excluidos;'

        df = pd.read_sql_query(query, self.conn)

        self.desconectar()

        return df

    
    # return retorno


executa_crud = ExecutaCrud()

def cadastrar_usuario():
    responsavel = st.session_state.username
    hashed = bcrypt.hashpw(new_pass.encode(), bcrypt.gensalt()).decode()
    df = pd.DataFrame({'usuario': [new_user], 'senha': [hashed], 'master': [check], 'responsavel': [responsavel], 'criado_em': datetime.now()})

    retorno = executa_crud.inserir(df)

    st.session_state.cad_user = ""
    st.session_state.cad_pwd = ""
    st.session_state.cad_rpwd = ""
    st.session_state.cad_check = False

    st.code(retorno)

def alterar_senha():
    responsavel = st.session_state.username
    hashed = bcrypt.hashpw(new_pass.encode(), bcrypt.gensalt()).decode()
    df = pd.DataFrame({'usuario': [user], 'senha': [hashed], 'master': [check], 'responsavel': responsavel, 'atualizado_em': datetime.now()})

    retorno = executa_crud.alterar(df)

    st.session_state.alt_user = ""
    st.session_state.alt_pwd = ""
    st.session_state.alt_rpwd = ""
    st.session_state.alt_check = False

    st.code(retorno)

def excluir_usuario():
    responsavel = st.session_state.username
    df = pd.DataFrame({'usuario': [user], 'responsavel': [responsavel]})

    retorno = executa_crud.excluir(df)

    st.session_state.exc_user = ""

    st.code(retorno)

def consultar_usuarios():
    df = executa_crud.consultar()

    st.dataframe(df)

def consultar_excluidos():
    df = executa_crud.consultar_excluidos()

    st.dataframe(df)
    

st.title("👑 Gerenciamento de Usuários")

with st.sidebar:
    st.title('Cadastro')

    ##### FILTRO DE CRUD #####
    if "selecao_cadastro" not in st.session_state:
        st.session_state.selecao_cadastro = "Cadastrar novo Usuário"

    lista_opcao = ["Cadastrar novo Usuário", "Alterar Senha", "Excluir Usuário", "Visualizar Usuários", "Visualizar Excluídos"]

    seleciona_crud = st.radio(
        'Selecione Opção',
        lista_opcao,
        key='selecao_cadastro'
    )

if seleciona_crud == 'Cadastrar novo Usuário':
    st.write("### Cadastrar novo Usuário.")
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
    st.write("### Alterar Senha.")
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
    st.write("### Excluir Usuário.")
    user = st.text_input("Usuário a ser excluído", key='exc_user')
    
    st.button("Excluir Usuário", on_click=excluir_usuario)

elif seleciona_crud == 'Visualizar Usuários':
    st.write("### Visualizar Usuários.")
    consultar_usuarios()

elif seleciona_crud == 'Visualizar Excluídos':
    st.write("### Visualizar Excluídos.")
    consultar_excluidos()
