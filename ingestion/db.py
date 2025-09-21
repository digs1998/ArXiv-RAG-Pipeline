# ingestion/db.py
from sqlalchemy import create_engine, Column, Integer, String, Text, Date, DateTime, ForeignKey, func
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
import os
from datetime import datetime

DATABASE_URL = os.getenv("POSTGRES_DATABASE_URL")  # required

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
Base = declarative_base()


class Paper(Base):
    __tablename__ = "papers"
    id = Column(Integer, primary_key=True)
    arxiv_id = Column(String(100), unique=True, index=True, nullable=False)
    title = Column(Text)
    authors = Column(Text)
    abstract = Column(Text)
    pdf_url = Column(Text)
    published_date = Column(Date)
    pdf_path = Column(Text)
    created_at = Column(DateTime, default=func.now())

    chunks = relationship("Chunk", back_populates="paper", cascade="all, delete-orphan")


class Chunk(Base):
    __tablename__ = "chunks"
    id = Column(Integer, primary_key=True)
    paper_id = Column(Integer, ForeignKey("papers.id"), index=True, nullable=False)
    text = Column(Text)
    chunk_idx = Column(Integer)
    created_at = Column(DateTime, default=func.now())
    section = Column(String, nullable=True)

    paper = relationship("Paper", back_populates="chunks")


def init_db():
    Base.metadata.create_all(bind=engine)


def upsert_paper(session, meta: dict, pdf_path: str = None):
    """
    meta: dict with keys arxiv_id, title, authors, abstract, published_date, pdf_url
    """
    p = session.query(Paper).filter_by(arxiv_id=meta["arxiv_id"]).one_or_none()
    if p is None:
        p = Paper(
            arxiv_id=meta["arxiv_id"],
            title=meta.get("title"),
            authors=meta.get("authors"),
            abstract=meta.get("abstract"),
            published_date=meta.get("published_date"),
            pdf_url=meta.get("pdf_url"),
            pdf_path=pdf_path,
        )
        session.add(p)
        session.flush()  # get p.id
    else:
        # update fields if changed
        p.title = meta.get("title")
        p.authors = meta.get("authors")
        p.abstract = meta.get("abstract")
        p.pdf_url = meta.get("pdf_url")
        if pdf_path:
            p.pdf_path = pdf_path
    return p
