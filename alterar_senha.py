import streamlit as st
import pandas as pd
import bcrypt

from querys.connect import Conexao


if "authenticated" not in st.session_state:
    st.error("Acesso negado. Faça login.")
    st.stop()

# verifica master
if not st.session_state.username:  
    st.error("Acesso restrito ao usuário master.")
    st.stop()

def executa_crud(df):
    conectar = Conexao()

    conectar.conectar_postgres()
    conn = conectar.obter_conexao_postgres()

    cur = conn.cursor()

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
    
    conectar.desconectar_postgres()
    
    return retorno

st.title(":key: Alterar Senha")

user = st.session_state.username
new_pass = st.text_input("Nova Senha", type="password")
repete_pass = st.text_input("Repita Nova Senha", type="password")

habilita = True
if new_pass == repete_pass and (new_pass != '' or repete_pass != '') and user != '':
        habilita = False
elif new_pass != repete_pass:
    st.code('Senhas diferentes.')

if st.button("Alterar Senha", disabled=habilita):
    hashed = bcrypt.hashpw(new_pass.encode(), bcrypt.gensalt()).decode()
    
    df = pd.DataFrame({'usuario': [user], 'senha': [hashed], 'master': [False]})

    retorno = executa_crud(df)

    st.code(retorno)
