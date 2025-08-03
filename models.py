"""Database models for the RAG system."""

from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Document(Base):  # type: ignore
    """Document model for storing documents and their embeddings."""

    __tablename__ = "documents"

    id = Column(Integer, primary_key=True, index=True)
    content = Column(Text, nullable=False)
    metadata_json = Column(JSON, nullable=True)  # JSON data
    embedding = Column(String, nullable=True)  # Vector will be stored as string
    filename = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    def __repr__(self) -> str:
        return f"<Document(id={self.id}, filename='{self.filename}')>"


class Company(Base):  # type: ignore
    """Company model for storing company information."""

    __tablename__ = "companies"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, nullable=True)
    exchange = Column(String, nullable=True)
    name = Column(String, nullable=False)

    # Relationships
    filings = relationship("Filing", back_populates="company")

    # Constraints
    __table_args__ = (
        UniqueConstraint("ticker", "exchange", name="uq_companies_ticker_exchange"),
    )

    def __repr__(self) -> str:
        return f"<Company(id={self.id}, ticker='{self.ticker}', name='{self.name}')>"


class Filing(Base):  # type: ignore
    """Filing model for storing SEC filing information."""

    __tablename__ = "filings"

    id = Column(Integer, primary_key=True, index=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    source = Column(String, nullable=False)
    filing_number = Column(String, nullable=False)
    form_type = Column(String, nullable=False)
    filing_date = Column(Date, nullable=False)
    fiscal_period_end = Column(Date, nullable=False)
    fiscal_year = Column(Integer, nullable=False)
    fiscal_quarter = Column(Integer, nullable=False)
    public_url = Column(String, nullable=True)

    # Relationships
    company = relationship("Company", back_populates="filings")
    financial_facts = relationship("FinancialFact", back_populates="filing")

    # Constraints
    __table_args__ = (
        UniqueConstraint(
            "source", "filing_number", name="uq_filings_source_filing_number"
        ),
    )

    def __repr__(self) -> str:
        return f"<Filing(id={self.id}, form_type='{self.form_type}', filing_date='{self.filing_date}')>"


class FinancialFact(Base):  # type: ignore
    """FinancialFact model for storing financial data from XBRL filings."""

    __tablename__ = "financial_facts"

    id = Column(BigInteger, primary_key=True, index=True)
    filing_id = Column(Integer, ForeignKey("filings.id"), nullable=False)
    taxonomy = Column(String, nullable=False)  # e.g., us-gaap
    tag = Column(String, nullable=False)  # e.g., Revenues
    value = Column(Numeric, nullable=True)
    unit = Column(String, nullable=True)  # e.g., USD
    section = Column(String, nullable=True)
    start_date = Column(Date, nullable=True)
    end_date = Column(Date, nullable=True)

    # Relationships
    filing = relationship("Filing", back_populates="financial_facts")

    def __repr__(self) -> str:
        return f"<FinancialFact(id={self.id}, tag='{self.tag}', value={self.value})>"
