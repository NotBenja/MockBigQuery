from pydantic import BaseModel, Field
from typing import List, Optional
from uuid import UUID, uuid4
from datetime import date

# ============================================================
# MODELOS PRINCIPALES
# ============================================================

class DataExtractionResponse(BaseModel):
    """Modelo para respuestas de extracción de datos"""
    id: UUID = Field(default_factory=uuid4)
    title: str
    summary: str
    date: date
    tags: List[str] = []
    pros: List[str] = []
    cons: List[str] = []
    authors: List[str] = []

    class Config:
        json_schema_extra = {
            "example": {
                "title": "Análisis de Mercado Q1 2025",
                "summary": "Resumen del análisis...",
                "date": "2025-01-15",
                "tags": ["Equity", "Technology"],
                "pros": ["Crecimiento sólido"],
                "cons": ["Alta volatilidad"],
                "authors": ["Analyst 1"]
            }
        }

class TradeIdea(BaseModel):
    """Modelo para ideas de trading"""
    id: UUID = Field(default_factory=uuid4)
    recommendation: str
    summary: str
    conviction: int = Field(ge=1, le=10)
    pros: List[str] = []
    cons: List[str] = []
    data_extraction_id: UUID

    class Config:
        json_schema_extra = {
            "example": {
                "recommendation": "COMPRAR",
                "summary": "Posición larga en tecnología",
                "conviction": 8,
                "pros": ["Innovación en IA"],
                "cons": ["Riesgo regulatorio"],
                "data_extraction_id": "550e8400-e29b-41d4-a716-446655440000"
            }
        }

# ============================================================
# MODELOS DE CONSULTA
# ============================================================

class DashboardQueryRequest(BaseModel):
    """Request para consulta del dashboard"""
    tags: Optional[List[str]] = None
    startDate: Optional[str] = None  # Formato: YYYY-MM-DD
    endDate: Optional[str] = None    # Formato: YYYY-MM-DD

class DataExtractionWithTradeIdeas(BaseModel):
    """Data extraction con sus trade ideas asociadas"""
    extraction: DataExtractionResponse
    trade_ideas: List[TradeIdea]

class PopularTag(BaseModel):
    """Tag popular con cantidad de documentos"""
    name: str
    count: int

class GraphData(BaseModel):
    """Datos para gráficos"""
    label: str
    value: int

class DashboardResponse(BaseModel):
    """Respuesta del dashboard con estadísticas completas"""
    total_extractions: int
    total_trade_ideas: int
    date_range: dict
    tags_filter: List[str] = []
    popular_tags: List[PopularTag] = []
    by_country: List[GraphData] = []
    by_sector: List[GraphData] = []
    results: List[DataExtractionWithTradeIdeas]

# ============================================================
# MODELOS DE RESPUESTA GENÉRICOS
# ============================================================

class DataExtractionListResponse(BaseModel):
    """Respuesta con lista de data extractions"""
    total: int
    items: List[DataExtractionResponse]

class TradeIdeaListResponse(BaseModel):
    """Respuesta con lista de trade ideas"""
    total: int
    items: List[TradeIdea]

# ============================================================
# MODELOS DE BASE DE DATOS
# ============================================================

from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class Tag(Base):
    __tablename__ = "tags"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False, index=True)
    category = Column(String, nullable=False, index=True)  # assetClass, region, country, sector, trade, counterpart
    
    created_at = Column(DateTime, default=datetime.utcnow)

class DataExtractionTag(Base):
    """Tabla intermedia para relación muchos-a-muchos entre DataExtraction y Tag"""
    __tablename__ = "data_extraction_tags"
    
    id = Column(Integer, primary_key=True, index=True)
    data_extraction_id = Column(Integer, ForeignKey("data_extractions.id", ondelete="CASCADE"), nullable=False)
    tag_id = Column(Integer, ForeignKey("tags.id", ondelete="CASCADE"), nullable=False)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Índice único para evitar duplicados
    __table_args__ = (
        UniqueConstraint('data_extraction_id', 'tag_id', name='_extraction_tag_uc'),
    )
