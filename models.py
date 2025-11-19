from pydantic import BaseModel, Field
from typing import List, Optional
from uuid import UUID, uuid4
from datetime import datetime

# ============================================================
# SCHEMAS BASE
# ============================================================

class BulletPoint(BaseModel):
    """Punto de viñeta con título y cuerpo"""
    title: str
    body: str

class SuggestedTag(BaseModel):
    """Tag sugerido con grupo, nombre y clave"""
    group: str
    tag: str
    key: str

class ContentExtractionTags(BaseModel):
    """Tags de contenido extraído"""
    asset_class: List[str] = Field(default_factory=list)
    e_d: List[str] = Field(default_factory=list)
    region: List[str] = Field(default_factory=list)
    country: List[str] = Field(default_factory=list)
    sector: List[str] = Field(default_factory=list)
    trade: List[str] = Field(default_factory=list)

class Tags(ContentExtractionTags):
    """Tags completos incluyendo counterpart"""
    counterpart: str

class TradeIdea(BaseModel):
    """Idea de trade anidada dentro de ExtractionTaskResponse"""
    recommendation: str
    summary: List[BulletPoint] = Field(default_factory=list)
    conviction: int
    pros: List[str] = Field(default_factory=list)
    cons: List[str] = Field(default_factory=list)

# ============================================================
# RESPONSE PRINCIPAL
# ============================================================

class ExtractionTaskResponse(BaseModel):
    """Schema principal - reemplaza DataExtractionResponse"""
    id: UUID = Field(default_factory=uuid4)
    title: str
    published_date: Optional[str] = None
    authors: List[str] = Field(default_factory=list)
    summary: List[BulletPoint] = Field(default_factory=list)
    tags: Tags
    pros: List[str] = Field(default_factory=list)
    cons: List[str] = Field(default_factory=list)
    trade_ideas: List[TradeIdea] = Field(default_factory=list)
    suggested_tags: List[SuggestedTag] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.now)
    deleted_at: Optional[datetime] = Field(default=None)

    class Config:
        json_schema_extra = {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "title": "Japan Economic Outlook Q4 2025",
                "published_date": "2025-11-09",
                "authors": ["John Doe", "Jane Smith"],
                "summary": [
                    {
                        "title": "Main Point",
                        "body": "Japan's economy shows resilience..."
                    }
                ],
                "tags": {
                    "counterpart": "Goldman Sachs",
                    "asset_class": ["Equity"],
                    "e_d": ["Developed"],
                    "region": ["Asia Pacific"],
                    "country": ["Japan"],
                    "sector": ["Technology"],
                    "trade": ["Long"]
                },
                "pros": ["Strong GDP growth", "Low inflation"],
                "cons": ["Aging population"],
                "trade_ideas": [
                    {
                        "recommendation": "Long JPY/USD",
                        "summary": [
                            {
                                "title": "Rationale",
                                "body": "BOJ policy shift expected..."
                            }
                        ],
                        "conviction": 8,
                        "pros": ["Central bank support"],
                        "cons": ["Political uncertainty"]
                    }
                ],
                "suggested_tags": [
                    {
                        "group": "sector",
                        "tag": "Semiconductors",
                        "key": "semiconductors"
                    }
                ]
            }
        }

# ============================================================
# RESPONSE WRAPPERS
# ============================================================

class ExtractionTaskListResponse(BaseModel):
    """Lista de extracciones"""
    total: int
    items: List[ExtractionTaskResponse]

class DashboardQueryRequest(BaseModel):
    """Request para consultas de dashboard"""
    tags: Optional[List[str]] = None
    startDate: Optional[str] = None
    endDate: Optional[str] = None

class DashboardResponse(BaseModel):
    """Response del dashboard con estadísticas"""
    total_extractions: int
    total_trade_ideas: int
    date_range: dict
    tags_filter: List[str]
    popular_tags: List[dict]
    by_country: List[dict]
    by_sector: List[dict]
    results: List[ExtractionTaskResponse]

# ============================================================
# TAG MODELS (mantener separados)
# ============================================================

class Tag(BaseModel):
    """Tag individual"""
    id: UUID = Field(default_factory=uuid4)
    name: str
    category: str

class TagListResponse(BaseModel):
    """Lista de tags"""
    total: int
    items: List[Tag]
