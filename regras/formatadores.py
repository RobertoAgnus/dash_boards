

def formatar_telefone(df, coluna_telefone):
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

def formatar_cpf(df, coluna_cpf):
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
