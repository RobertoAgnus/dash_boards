from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, text

from conexoes.database import Conexao

Base = declarative_base()
SCHEMA = "digisac"

class Disparados(Base):
    __tablename__  = 'disparados'
    __table_args__ = {"schema": SCHEMA}

    number       = Column(String, primary_key=True)
    qtd_disparos = Column(Integer)
    dt_disparo   = Column(DateTime())
    tag          = Column(String)

conexao = Conexao()

engine = conexao.engine_postgres

# cria o schema se não existir
with engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
    
Base.metadata.create_all(engine)