from fastapi import Body, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from models import (
    ExtractionTaskResponse,
    ExtractionTaskListResponse,
    DashboardQueryRequest,
    DashboardResponse,
    Tag,
    TagListResponse
)
from database import DuckDBClient
from typing import List, Optional
from uuid import UUID, uuid4
import os
import uvicorn
from datetime import datetime
from pathlib import Path
import json

# ============================================================
# CONFIGURACIÓN DE LA APP
# ============================================================

app = FastAPI(
    title="MockBigQuery - Research Data API v3",
    description="API consolidada para gestión de research extractions con trade ideas anidados",
    version="3.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS
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
# ENDPOINTS DE SALUD
# ============================================================

@app.get("/")
def root():
    """Endpoint raíz con información de la API"""
    return {
        "service": "MockBigQuery - Research Data API",
        "version": "3.0.0",
        "status": "online",
        "changes": {
            "v3.0.0": [
                "✅ Tabla consolidada research_extractions",
                "✅ Trade ideas anidados (no tabla separada)",
                "✅ Summary como List[BulletPoint]",
                "✅ Suggested tags incluidos",
                "✅ Tabla tags separada mantenida",
                "❌ Eliminada tabla trade_ideas independiente"
            ]
        },
        "endpoints": {
            "health": "GET /health",
            "docs": "GET /docs",
            "extractions": {
                "list": "GET /api/extractions",
                "create": "POST /api/extractions",
                "get_by_id": "GET /api/extractions/{id}"
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
        result = db.execute("SELECT 1 as test")
        return {
            "status": "healthy",
            "database": "connected",
            "version": "3.0.0"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e)
        }

# ============================================================
# ENDPOINTS DE EXTRACTIONS
# ============================================================

@app.get("/api/extractions", response_model=ExtractionTaskListResponse)
def list_extractions(
    tags: Optional[str] = None,  # Formato: "tag1,tag2,tag3"
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    limit: Optional[int] = None
):
    """
    Lista extractions con filtros opcionales
    
    Query params:
    - tags: Tags separados por comas (OR logic)
    - startDate: Fecha inicial (YYYY-MM-DD)
    - endDate: Fecha final (YYYY-MM-DD)
    - limit: Límite de resultados
    """
    try:
        tags_list = tags.split(",") if tags else None
        
        results = db.get_extractions(
            tags=tags_list,
            start_date=startDate,
            end_date=endDate,
            limit=limit
        )
        
        items = [ExtractionTaskResponse(**row) for row in results]
        
        return ExtractionTaskListResponse(total=len(items), items=items)
        
    except Exception as e:
        print(f"❌ Error listing extractions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/extractions/{extraction_id}", response_model=ExtractionTaskResponse)
def get_extraction(extraction_id: UUID):
    """Obtiene una extraction por ID"""
    try:
        result = db.get_extraction_by_id(str(extraction_id))
        
        if not result:
            raise HTTPException(status_code=404, detail="Extraction not found")
        
        return ExtractionTaskResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error getting extraction: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/extractions", response_model=ExtractionTaskResponse, status_code=201)
def create_extraction(extraction: ExtractionTaskResponse):
    """Crea una nueva extraction con trade ideas anidados"""
    try:
        data = extraction.model_dump()
        result = db.insert_extraction(data)
        
        if not result:
            raise HTTPException(status_code=500, detail="Failed to create extraction")
        
        return ExtractionTaskResponse(**result)
        
    except Exception as e:
        print(f"❌ Error creating extraction: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.patch("/api/extractions/{extraction_id}", response_model=ExtractionTaskResponse)
def soft_delete_extraction(
    extraction_id: UUID,
    deleted_at: Optional[str] = Body(..., embed=True)
):
    """
    Soft delete de una extraction actualizando el campo deleted_at

    Body esperado:
    {
        "deleted_at": "2025-01-01T10:30:00"
    }

    Si deleted_at es null, se restaura el documento.
    """
    try:
        extraction = db.get_extraction_by_id(str(extraction_id))
        if not extraction:
            raise HTTPException(status_code=404, detail="Extraction not found")

        if deleted_at is None:
            deleted_at = datetime.now().isoformat()
        
        extraction["deleted_at"] = deleted_at

        updated = db.update_extraction_deleted_at(str(extraction_id), deleted_at)

        if not updated:
            raise HTTPException(status_code=500, detail="Failed to update deleted_at field")
        
        return ExtractionTaskResponse(**extraction)

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating deleted_at: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.patch("/api/extractions/{extraction_id}/trade-ideas/{trade_idea_id}")
def soft_delete_trade_idea(
    extraction_id: UUID,
    trade_idea_id: UUID,
    deleted_at: Optional[str] = Body(..., embed=True)
):
    """
    Soft delete de un trade idea específico
    
    Body:
    {
        "deleted_at": "2025-11-19T10:30:00" o null para restaurar
    }
    """
    try:
        # Obtener extraction
        extraction = db.get_extraction_by_id(str(extraction_id), include_deleted=True)
        if not extraction:
            raise HTTPException(status_code=404, detail="Extraction not found")
        
        # Buscar trade idea
        trade_ideas = extraction.get('trade_ideas', [])
        trade_idea_found = False
        
        for trade_idea in trade_ideas:
            if str(trade_idea.get('id')) == str(trade_idea_id):
                trade_idea['deleted_at'] = deleted_at
                trade_idea_found = True
                break
        
        if not trade_idea_found:
            raise HTTPException(status_code=404, detail="Trade idea not found")
        
        # Actualizar extraction completa con trade_ideas modificados
        updated = db.update_extraction_trade_ideas(str(extraction_id), trade_ideas)
        
        if not updated:
            raise HTTPException(status_code=500, detail="Failed to update trade idea")
        
        return {
            "status": "success",
            "extraction_id": str(extraction_id),
            "trade_idea_id": str(trade_idea_id),
            "deleted_at": deleted_at
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error updating trade idea: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# ENDPOINT DE DASHBOARD
# ============================================================

@app.post("/api/dashboard", response_model=DashboardResponse)
def query_dashboard(request: DashboardQueryRequest):
    """
    Consulta dashboard con extractions y estadísticas
    
    Body:
    {
        "tags": ["tag1", "tag2"],  // Opcional
        "startDate": "2025-01-01",  // Opcional
        "endDate": "2025-12-31"     // Opcional
    }
    """
    try:
        print(f"📊 Dashboard query: tags={request.tags}, dates={request.startDate} to {request.endDate}")
        
        # Obtener extractions
        extractions = db.get_extractions(
            tags=request.tags,
            start_date=request.startDate,
            end_date=request.endDate
        )
        
        # Contar trade ideas totales
        total_trade_ideas = sum(len(e.get('trade_ideas', [])) for e in extractions)
        
        # Convertir a Pydantic models
        results = [ExtractionTaskResponse(**e) for e in extractions]
        
        # Obtener estadísticas
        popular_tags_data = db.get_popular_tags(
            tag_names=request.tags,
            start_date=request.startDate,
            end_date=request.endDate,
            limit=5
        )
        
        by_country_data = db.get_extractions_by_country(
            tag_names=request.tags,
            start_date=request.startDate,
            end_date=request.endDate
        )
        
        by_sector_data = db.get_extractions_by_sector(
            tag_names=request.tags,
            start_date=request.startDate,
            end_date=request.endDate
        )
        
        print(f"✅ Found {len(results)} extractions with {total_trade_ideas} trade ideas")
        
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

@app.get("/api/tags", response_model=TagListResponse)
def get_all_tags():
    """Obtiene todos los tags disponibles"""
    try:
        tags = db.get_all_tags()
        items = [Tag(**tag) for tag in tags]
        
        return TagListResponse(total=len(items), items=items)
        
    except Exception as e:
        print(f"❌ Error getting tags: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tags/by-category/{category}", response_model=TagListResponse)
def get_tags_by_category(category: str):
    """Obtiene tags por categoría"""
    try:
        tags = db.get_tags_by_category(category)
        items = [Tag(**tag) for tag in tags]
        
        return TagListResponse(total=len(items), items=items)
        
    except Exception as e:
        print(f"❌ Error getting tags by category: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tags/categories")
def get_tag_categories():
    """Obtiene categorías de tags con conteos"""
    try:
        categories = db.execute("""
            SELECT 
                category,
                COUNT(*) as tag_count
            FROM tags
            GROUP BY category
            ORDER BY category
        """)
        
        return categories
        
    except Exception as e:
        print(f"❌ Error getting categories: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dumpdata")
def dump_data():
    """
    Exporta todas las extractions a JSON y las guarda en mock_data/extractions.json
    """
    try:
        # ✅ VERIFICAR: ¿Qué devuelve get_extractions?
        extractions = db.get_extractions(include_deleted=True)
        
        print(f"📊 Total extractions obtenidas: {len(extractions)}")
        
        if not extractions:
            raise HTTPException(status_code=404, detail="No extractions found")
        
        # ✅ AGREGAR: Validación de datos antes de guardar
        valid_extractions = []
        for idx, extraction in enumerate(extractions):
            # Verificar que la extraction tenga al menos un título
            if extraction.get('title'):
                # Asegurar que trade ideas tengan id
                if 'trade_ideas' in extraction and isinstance(extraction['trade_ideas'], list):
                    for trade_idea in extraction['trade_ideas']:
                        if 'id' not in trade_idea or not trade_idea['id']:
                            trade_idea['id'] = str(uuid4())
                        if 'deleted_at' not in trade_idea:
                            trade_idea['deleted_at'] = None
                
                valid_extractions.append(extraction)
            else:
                print(f"⚠️  Extraction {idx} sin título, se omite")
        
        print(f"✅ Extractions válidas: {len(valid_extractions)}")
        
        # Crear estructura de dump
        dump_data = {
            "exported_at": datetime.now().isoformat(),
            "total": len(valid_extractions),
            "version": "3.0.0",
            "extractions": valid_extractions
        }
        
        # Crear directorio si no existe
        dump_dir = Path("mock_data")
        dump_dir.mkdir(exist_ok=True)
        
        # Guardar archivo (SOBRESCRIBIR)
        dump_file = dump_dir / "extractions.json"
        
        with open(dump_file, 'w', encoding='utf-8') as f:
            json.dump(dump_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"✅ Dump creado: {dump_file}")
        print(f"📊 Total extractions exportadas: {len(extractions)}")
        
        db.close()
        
        return {
            "status": "success",
            "file": str(dump_file),
            "total_extractions": len(extractions),
            "exported_at": datetime.now().isoformat(),
            "version": "3.0.0"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error dumping data: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error al exportar datos: {str(e)}")

# ============================================================
# SERVIDOR
# ============================================================

if __name__ == "__main__":
    host = os.environ.get("HOST", "localhost")
    port = int(os.environ.get("PORT", 9000))
    
    print(f"🚀 Starting MockBigQuery v3.0.0 on {host}:{port}")
    uvicorn.run(app, host=host, port=port)
