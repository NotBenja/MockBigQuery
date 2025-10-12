from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from uuid import UUID, uuid4
from datetime import date

# ============================================================
# MODELOS DE DATOS (Domain Models)
# ============================================================

class DataExtractionResponse(BaseModel):
    """Modelo para respuestas de extracción de datos de investigación"""
    id: Optional[UUID] = Field(default_factory=uuid4)
    title: str
    summary: str
    date: date
    tags: List[str] = Field(default_factory=list)
    pros: List[str] = Field(default_factory=list)
    cons: List[str] = Field(default_factory=list)
    authors: List[str] = Field(default_factory=list)
    
    class Config:
        json_schema_extra = {
            "example": {
                "title": "Análisis de Mercado Q4 2024",
                "summary": "Resumen del análisis...",
                "date": "2024-12-15",
                "tags": ["tecnología", "IA"],
                "pros": ["Crecimiento fuerte", "Valoraciones atractivas"],
                "cons": ["Alta volatilidad", "Riesgo regulatorio"],
                "authors": ["Juan Pérez", "María González"]
            }
        }


class TradeIdea(BaseModel):
    """Modelo para ideas de trading"""
    id: Optional[UUID] = Field(default_factory=uuid4)
    recommendation: str
    summary: str
    conviction: int = Field(ge=1, le=10)  # Convicción entre 1 y 10
    pros: List[str] = Field(default_factory=list)
    cons: List[str] = Field(default_factory=list)
    data_extraction_id: Optional[UUID] = None  # Foreign key a DataExtractionResponse
    
    class Config:
        json_schema_extra = {
            "example": {
                "recommendation": "COMPRAR",
                "summary": "Posición larga en tecnología...",
                "conviction": 8,
                "pros": ["Liderazgo en IA", "Flujos de caja robustos"],
                "cons": ["Riesgo regulatorio", "Alta competencia"],
                "data_extraction_id": "123e4567-e89b-12d3-a456-426614174000"
            }
        }


# ============================================================
# MODELOS DE API (Request/Response Models)
# ============================================================

class QueryRequest(BaseModel):
    sql: str


class InsertRequest(BaseModel):
    table: str
    data: List[Dict[str, Any]]
    id_field: Optional[str] = "id"


class CreateTableRequest(BaseModel):
    name: str
    table_schema: str
    primary_key: Optional[str] = None


# ============================================================
# MODELOS DE RESPUESTA
# ============================================================

class DataExtractionListResponse(BaseModel):
    """Respuesta con lista de extracciones de datos"""
    total: int
    items: List[DataExtractionResponse]


class TradeIdeaListResponse(BaseModel):
    """Respuesta con lista de ideas de trading"""
    total: int
    items: List[TradeIdea]


class InsertResponse(BaseModel):
    """Respuesta de inserción"""
    status: str
    rows: int
    message: str
    ids: Optional[List[str]] = None


class CreateTableResponse(BaseModel):
    """Respuesta de creación de tabla"""
    status: str
    table: str
    schema: str
