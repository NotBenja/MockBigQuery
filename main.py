from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from models import (
    DataExtractionResponse,
    TradeIdea,
    DataExtractionListResponse,
    TradeIdeaListResponse,
    DashboardQueryRequest,
    DashboardResponse,
    DataExtractionWithTradeIdeas
)
from database import DuckDBClient
from typing import List
from uuid import UUID
import json
import os
import uvicorn

# ============================================================
# CONFIGURACIÓN DE LA APP
# ============================================================

app = FastAPI(
    title="MockBigQuery - Research Data API",
    description="API simplificada para gestión de research data y trade ideas",
    version="2.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS (ajusta según necesites)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inicializar base de datos
db = DuckDBClient()

# ============================================================
# HELPERS
# ============================================================

def parse_json_fields(row: dict, fields: List[str]) -> dict:
    """Parsea campos JSON de strings a objetos Python"""
    for field in fields:
        if field in row and isinstance(row[field], str):
            try:
                row[field] = json.loads(row[field])
            except json.JSONDecodeError:
                row[field] = []
    return row

# ============================================================
# ENDPOINTS DE SALUD
# ============================================================

@app.get("/")
def root():
    """Endpoint raíz con información de la API"""
    return {
        "service": "MockBigQuery - Research Data API",
        "version": "2.1.0",
        "status": "online",
        "endpoints": {
            "health": "GET /health",
            "docs": "GET /docs",
            "data_extractions": {
                "list": "GET /api/data-extractions",
                "create": "POST /api/data-extractions",
                "get_by_id": "GET /api/data-extractions/{id}",
                "get_trade_ideas": "GET /api/data-extractions/{id}/trade-ideas"
            },
            "trade_ideas": {
                "create": "POST /api/trade-ideas"
            },
            "dashboard": {
                "query": "POST /api/dashboard"
            },
            "tags": {
                "get_all": "GET /api/tags",
                "get_by_category": "GET /api/tags/by-category/{category}",
                "get_categories": "GET /api/tags/categories"
            }
        }
    }

@app.get("/health")
def health_check():
    """Health check endpoint"""
    try:
        # Verificar conexión a DB
        result = db.execute("SELECT 1 as test")
        return {
            "status": "healthy",
            "database": "connected",
            "version": "2.1.0"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e)
        }

# ============================================================
# ENDPOINTS DE DATA EXTRACTIONS
# ============================================================

@app.get("/api/data-extractions", response_model=DataExtractionListResponse)
def list_data_extractions(
    tags: str = None,  # Formato: "tag1,tag2,tag3"
    startDate: str = None,
    endDate: str = None
):
    """
    Lista data extractions con filtros opcionales
    
    Query params:
    - tags: Tags separados por comas (OR logic)
    - startDate: Fecha inicial (YYYY-MM-DD)
    - endDate: Fecha final (YYYY-MM-DD)
    """
    try:
        # Parsear tags
        tags_list = tags.split(",") if tags else None
        
        # Consultar DB
        results = db.get_data_extractions(
            tags=tags_list,
            start_date=startDate,
            end_date=endDate
        )
        
        # Parsear JSON fields
        items = []
        for row in results:
            row = parse_json_fields(row, ['tags', 'pros', 'cons', 'authors'])
            items.append(DataExtractionResponse(**row))
        
        return DataExtractionListResponse(total=len(items), items=items)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/data-extractions/{extraction_id}", response_model=DataExtractionResponse)
def get_data_extraction(extraction_id: UUID):
    """Obtiene un data extraction por ID"""
    try:
        query = f"SELECT * FROM data_extraction_responses WHERE id = '{extraction_id}'"
        result = db.execute(query)
        
        if isinstance(result, dict) and "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
        
        if not result:
            raise HTTPException(status_code=404, detail="Data extraction not found")
        
        row = parse_json_fields(result[0], ['tags', 'pros', 'cons', 'authors'])
        return DataExtractionResponse(**row)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/data-extractions", response_model=DataExtractionResponse, status_code=201)
def create_data_extraction(extraction: DataExtractionResponse):
    """Crea un nuevo data extraction"""
    try:
        data = extraction.model_dump()
        result = db.insert_data_extraction(data)
        
        row = parse_json_fields(result, ['tags', 'pros', 'cons', 'authors'])
        return DataExtractionResponse(**row)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/data-extractions/{extraction_id}/trade-ideas", response_model=TradeIdeaListResponse)
def get_trade_ideas_by_extraction(extraction_id: UUID):
    """Obtiene todas las trade ideas de un data extraction"""
    try:
        results = db.get_trade_ideas_by_extraction(str(extraction_id))
        
        items = []
        for row in results:
            row = parse_json_fields(row, ['pros', 'cons'])
            items.append(TradeIdea(**row))
        
        return TradeIdeaListResponse(total=len(items), items=items)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# ENDPOINTS DE TRADE IDEAS
# ============================================================

@app.post("/api/trade-ideas", response_model=TradeIdea, status_code=201)
def create_trade_idea(idea: TradeIdea):
    """Crea una nueva trade idea"""
    try:
        data = idea.model_dump()
        result = db.insert_trade_idea(data)
        
        row = parse_json_fields(result, ['pros', 'cons'])
        return TradeIdea(**row)
        
    except Exception as e:
        # Si es error de foreign key, dar mensaje más claro
        if "foreign key" in str(e).lower():
            raise HTTPException(
                status_code=400,
                detail=f"Data extraction with id {idea.data_extraction_id} does not exist"
            )
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# ENDPOINT DE DASHBOARD
# ============================================================

@app.post("/api/dashboard", response_model=DashboardResponse)
def query_dashboard(request: DashboardQueryRequest):
    """
    Consulta dashboard con data extractions y sus trade ideas
    
    Body:
    {
        "tags": ["tag1", "tag2"],  // Opcional
        "startDate": "2025-01-01",  // Opcional
        "endDate": "2025-12-31"     // Opcional
    }
    """
    try:
        print(f"📊 Dashboard query: tags={request.tags}, dates={request.startDate} to {request.endDate}")
        
        # Obtener data extractions
        extractions = db.get_data_extractions(
            tags=request.tags,
            start_date=request.startDate,
            end_date=request.endDate
        )
        
        # Procesar cada extraction y obtener sus trade ideas
        results = []
        total_trade_ideas = 0
        
        for extraction_row in extractions:
            # Parsear extraction
            extraction_row = parse_json_fields(extraction_row, ['tags', 'pros', 'cons', 'authors'])
            extraction = DataExtractionResponse(**extraction_row)
            
            # Obtener trade ideas
            trade_ideas_rows = db.get_trade_ideas_by_extraction(str(extraction.id))
            trade_ideas = []
            
            for idea_row in trade_ideas_rows:
                idea_row = parse_json_fields(idea_row, ['pros', 'cons'])
                trade_ideas.append(TradeIdea(**idea_row))
            
            total_trade_ideas += len(trade_ideas)
            
            results.append(DataExtractionWithTradeIdeas(
                extraction=extraction,
                trade_ideas=trade_ideas
            ))
        
        # ============================================================
        # OBTENER ESTADÍSTICAS ADICIONALES
        # ============================================================
        
        # Tags populares
        popular_tags_data = db.get_popular_tags(
            tag_names=request.tags,
            start_date=request.startDate,
            end_date=request.endDate,
            limit=5
        )
        
        # Extracciones por país
        by_country_data = db.get_extractions_by_country(
            tag_names=request.tags,
            start_date=request.startDate,
            end_date=request.endDate
        )
        
        # Extracciones por sector
        by_sector_data = db.get_extractions_by_sector(
            tag_names=request.tags,
            start_date=request.startDate,
            end_date=request.endDate
        )
        
        print(f"✅ Found {len(results)} extractions with {total_trade_ideas} trade ideas")
        print(f"📊 Stats: {len(popular_tags_data)} popular tags, {len(by_country_data)} countries, {len(by_sector_data)} sectors")
        
        return DashboardResponse(
            total_extractions=len(results),
            total_trade_ideas=total_trade_ideas,
            date_range={
                "start": request.startDate or "No especificada",
                "end": request.endDate or "No especificada"
            },
            tags_filter=request.tags or [],
            popular_tags=popular_tags_data,
            by_country=by_country_data,
            by_sector=by_sector_data,
            results=results
        )
        
    except Exception as e:
        print(f"❌ Dashboard error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# ENDPOINTS DE TAGS
# ============================================================

@app.get("/api/tags")
def get_all_tags():
    """
    Obtiene todos los tags disponibles en el sistema
    
    Returns:
        Lista de tags con sus categorías
    """
    try:
        tags = db.execute("""
            SELECT 
                id,
                name,
                category
            FROM tags
            ORDER BY category, name
        """)
        
        if isinstance(tags, dict) and "error" in tags:
            raise HTTPException(status_code=500, detail=tags["error"])
        
        if not tags:
            return []
        
        print(f"✅ Returning {len(tags)} tags")
        return tags
        
    except Exception as e:
        print(f"❌ Error al obtener tags: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener tags: {str(e)}"
        )

@app.get("/api/tags/by-category/{category}")
def get_tags_by_category(category: str):
    """
    Obtiene todos los tags de una categoría específica
    
    Args:
        category: Categoría del tag (asset_class, country, sector, etc.)
    
    Returns:
        Lista de tags de esa categoría
    """
    try:
        tags = db.execute(f"""
            SELECT 
                id,
                name,
                category
            FROM tags
            WHERE category = '{category}'
            ORDER BY name
        """)
        
        if isinstance(tags, dict) and "error" in tags:
            raise HTTPException(status_code=500, detail=tags["error"])
        
        if not tags:
            return []
        
        print(f"✅ Returning {len(tags)} tags for category '{category}'")
        return tags
        
    except Exception as e:
        print(f"❌ Error al obtener tags por categoría: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener tags: {str(e)}"
        )

@app.get("/api/tags/categories")
def get_tag_categories():
    """
    Obtiene todas las categorías de tags disponibles
    
    Returns:
        Lista de categorías con el conteo de tags en cada una
    """
    try:
        categories = db.execute("""
            SELECT 
                category,
                COUNT(*) as tag_count
            FROM tags
            GROUP BY category
            ORDER BY category
        """)
        
        if isinstance(categories, dict) and "error" in categories:
            raise HTTPException(status_code=500, detail=categories["error"])
        
        if not categories:
            return []
        
        print(f"✅ Returning {len(categories)} tag categories")
        return categories
        
    except Exception as e:
        print(f"❌ Error al obtener categorías: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener categorías: {str(e)}"
        )

# ============================================================
# SERVIDOR
# ============================================================

if __name__ == "__main__":
    host = os.environ.get("HOST", "localhost")
    port = int(os.environ.get("PORT", 9000))
    
    uvicorn.run(app, host=host, port=port)
