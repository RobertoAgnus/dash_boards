import re
import streamlit as st
from datetime import date


class Tratamentos:
    def __init__(self):
        ...

    ##### FUNÇÃO PARA OBTER AS DATAS #####
    def get_datas(self, df, coluna):
        # Remove linhas com Data Mensagem vazia
        df = df.dropna(subset=[coluna])

        # Obtendo a menor e a maior data da coluna 'data'
        menor_data = df[coluna].min()
        maior_data = date.today()
        
        return menor_data, maior_data


    ##### FUNÇÃO PARA MAPEAR MENSAGENS #####
    def mapeia_mensagens(self, mensagem):
        if '[' in str(mensagem):
            resultado = re.search(r'\[[^\]]+\]', mensagem)

            if (len(resultado.group()) < 10):
                return resultado.group() if resultado else None
            else:
                return "Orgânico"
            
        elif '(' in str(mensagem):
            resultado = re.search(r'\([^\)]+\)', mensagem)
            
            if (len(resultado.group()) < 9) and (len(resultado.group()) > 3):
                if re.search(r"[^\(0-9R$\)]", resultado.group()):
                    return resultado.group() if resultado else None
                else:
                    return "Orgânico"
            else:
                return "Orgânico"
        elif ("Olá! Gostaria" in str(mensagem)) |\
            ("Olá! Quero" in str(mensagem)) |\
            ("Olá! Tenho interesse" in str(mensagem)) |\
            ("Olá, Gostaria" in str(mensagem)) |\
            ("Olá, quero" in str(mensagem)):
            return "(site)"
        elif ("Falar com atendente" in str(mensagem)) |\
            ("Falar com suporte" in str(mensagem)) |\
            ("Ver atualização" in str(mensagem)) |\
            ("Receber proposta" in str(mensagem)):
            return "Disparos"
        else:
            return "Orgânico"
        
    ##### FUNÇÃO PARA MAPEAR AS CAMPANHAS #####
    def mapeia_campanha(self, valor):
        return valor.replace('[CAMPEÕES ', '[').replace('TRABALHA +1 ANO', 'CR+1').replace('CAIXA DE PERGUNTAS', 'CRCP').replace('CR ', 'CR')

    ##### FUNÇÃO PARA GERAR OS CARDS #####
    def metric_card(self, label, value):
        st.markdown(
            f"""
            <div style="
                background-color: #262730;
                border-radius: 10px;
                text-align: center;
                margin-bottom: 15px;
                height: auto;
            ">
                <p style="color: white; font-weight: bold; font-size: clamp(0.5rem, 1.2vw, 0.9rem)">{label}</p>
                <h3 style="color: white; font-size: clamp(0.5rem, 4vw, 1.5rem)">{value}</h3>
            </div>
            """,
            unsafe_allow_html=True
        )