import re
import unicodedata
import numpy as np
import pandas as pd
from datetime import date


class Regras:
    def __init__(self):
        ...

    def formatar_telefone(self, df, coluna_telefone):
        # Remove 55 dos telefones
        df[coluna_telefone] = df[coluna_telefone].astype(str).str.replace(
            r'^(55)(?=\d{11,})', 
            '', 
            regex=True
        )

        # Adiciona "9" depois do segundo dígito, se o número tiver apenas 10 dígitos
        df[coluna_telefone] = df[coluna_telefone].apply(
            lambda x: x[:2] + '9' + x[2:] if len(x) == 10 and x.isdigit() else x
        )

        # Substitui 'nan' e strings vazias por None
        df[coluna_telefone] = df[coluna_telefone].replace(['nan', ''], None)

        df[coluna_telefone] = df[coluna_telefone].astype(str).apply(
            lambda x: x[:2] + x[3:] if len(x) > 11 and x.isdigit() else x
        )

        return df

    def formatar_cpf(self, df, coluna_cpf):
        df[coluna_cpf] = (
            df[coluna_cpf]
            .astype(str)
            .str.replace(r'\D', '', regex=True)  # remove tudo que não é dígito
            .str.strip()                         # remove espaços
        )
        
        df[coluna_cpf] = df[coluna_cpf].astype(str).str.strip()

        df.loc[df[coluna_cpf].str.contains('opt|\[\]|None', case=False, na=False) |
        (df[coluna_cpf] == ''), coluna_cpf] = None
        
        df.loc[df[coluna_cpf].str.contains('Sem CPF', case=False, na=False), coluna_cpf] = None

        df[coluna_cpf] = df[coluna_cpf].str.zfill(11)

        return df

    ##### FUNÇÃO PARA REMOVER EMOJIS #####
    def remover_emojis(self, texto: str) -> str:
        if not isinstance(texto, str):
            return texto
        return "".join(
            c for c in texto
            if not unicodedata.category(c).startswith("So")
        )


    ##### FUNÇÃO PARA FORMATAR FLOAT #####
    def formata_float(self, valor):
        if valor is None or pd.isna(valor):
            return "0,00"

        return (
            f"{float(valor):,.2f}"
            .replace('.', '|')
            .replace(',', '.')
            .replace('|', ',')
        )


    ##### FUNÇÃO PARA OBTER O COMPRIMENTO DO NOME #####
    def tamanho_nome(self, nome):
        if isinstance(nome, str):
            return len(nome)
        return 0

    ##### FUNÇÃO PARA LIMPAR O NOME #####
    def limpar_nome(self, nome):
        if pd.isna(nome):
            return nome

        # Remove acentos
        nome = unicodedata.normalize('NFKD', nome)
        nome = nome.encode('ASCII', 'ignore').decode('ASCII')

        # Remove caracteres especiais (mantém letras e espaço)
        nome = re.sub(r'[^A-Za-z\s]', '', nome)

        # Remove espaços duplicados
        nome = re.sub(r'\s+', ' ', nome).strip()

        return nome

    ##### FUNÇÃO PARA LIMPAR CPF #####
    def limpar_cpf(self, cpf):
        cpf = str(cpf).replace('.', '').replace('-', '')
        return np.where(cpf == 'nan', None, cpf)

    ##### FUNÇÃO PARA TRATAR OS CÓDIGOS #####
    def trata_codigo(self, codigo):
        codigo = str(codigo).replace('.0', '')
        return np.where((codigo == 'None') | (codigo == 'nan'), None, codigo)
