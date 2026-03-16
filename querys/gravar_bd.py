import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from querys.tabelas import Disparados

from conexoes.database import Conexao

class GravarBandoDados:
    def __init__(self):
        self.conexao = Conexao()
        self.engine = self.conexao.engine_postgres

        ...

    # def gravar_disparados(self, objeto):
    #     con = Conexao()

    #     stmt = insert(Disparados).values(
    #         number             = objeto.name,
    #         qtd_disparos    = objeto.qtd_disparos,
    #         dt_disparo = pd.Timestamp.now(),
    #         tag           = objeto.tag,
    #         createdAt        = pd.Timestamp.now()
    #     ).on_conflict_do_update(
    #         index_elements=["number"],
    #         set_={
    #             "qtd_disparos"   : objeto.qtd_disparos,
    #             "dt_disparo": pd.Timestamp.now(),
    #             "tag"       : pd.Timestamp.now()
    #         }
    #     )

    #     session = con.get_conexao_postgres()
    #     session.execute(stmt)
    #     session.commit()
    #     session.close()


    def upsert_dataframe(self, df):
        df = df.drop_duplicates(subset=["number"])
        
        tabela = Disparados()
        table = tabela.__table__
        
        records = df.to_dict(orient="records")

        stmt = insert(Disparados).values(records)

        update_cols = {
            c.name: stmt.excluded[c.name]
            for c in table.columns
            if not c.primary_key
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=["number"],  # ou constraint="nome_constraint"
            set_=update_cols
        )

        with Session(self.engine) as session:
            session.execute(stmt)
            session.commit()