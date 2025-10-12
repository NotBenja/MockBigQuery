from fastapi import FastAPI, HTTPException, Query
from models import (
    QueryRequest, 
    InsertRequest, 
    CreateTableRequest,
    DataExtractionResponse,
    TradeIdea,
    DataExtractionListResponse,
    TradeIdeaListResponse,
    InsertResponse,
    CreateTableResponse
)
from database import DuckDBClient
from typing import Optional, List
from uuid import UUID
import os
import uvicorn
import pandas as pd
import json
from datetime import date
from pydantic import BaseModel

app = FastAPI(
    title="Mock BigQuery Service (DuckDB) - V2",
    description="Mock BigQuery con soporte para UUID, JSON nativo y Foreign Keys",
    version="2.0.0"
)
db = DuckDBClient()

@app.get("/api/health")
def health_check():
    """Verifica que el servicio esté funcionando"""
    return {"status": "ok"}

@app.get("/")
def root():
    return {
        "message": "Mock BigQuery V2 is running",
        "features": [
            "UUID primary keys",
            "JSON native types",
            "Foreign key constraints",
            "Type-safe Pydantic models"
        ]
    }

# ============================================================
# ENDPOINTS GENÉRICOS (compatibilidad con versión anterior)
# ============================================================

@app.post("/query")
def run_query(request: QueryRequest):
    result = db.execute(request.sql)
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return {"rows": result}

@app.post("/insert")
def insert_rows(request: InsertRequest) -> InsertResponse:
    df = pd.DataFrame(request.data)
    try:
        db.insert_dataframe(request.table, df, request.id_field)
        return InsertResponse(
            status="success",
            rows=len(df),
            message=f"Inserted {len(df)} rows into {request.table}",
            ids=[str(row[request.id_field]) for row in request.data if request.id_field in row]
        )
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/create_table")
async def create_table(request: CreateTableRequest) -> CreateTableResponse:
    """Crea una tabla nueva en la base de datos"""
    try:
        result = db.create_table(
            table_name=request.name,
            schema=request.table_schema,
            primary_key=request.primary_key
        )
        
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        
        return CreateTableResponse(
            status="success",
            table=request.name,
            schema=request.table_schema
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# ENDPOINTS ESPECÍFICOS PARA DATA EXTRACTION
# ============================================================

@app.get("/api/data-extractions", response_model=DataExtractionListResponse)
def get_data_extractions(
    limit: int = Query(default=10, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    tag: Optional[str] = None
):
    """Obtiene lista de data extractions con paginación y filtros"""
    
    # Construir query con filtros
    where_clause = ""
    if tag:
        where_clause = f"WHERE list_contains(tags, '{tag}')"
    
    # Obtener total
    count_query = f"SELECT COUNT(*) as total FROM data_extraction_responses {where_clause}"
    total_result = db.execute(count_query)
    total = total_result[0]["total"] if total_result else 0
    
    # Obtener datos
    query = f"""
        SELECT * FROM data_extraction_responses 
        {where_clause}
        ORDER BY date DESC
        LIMIT {limit} OFFSET {offset}
    """
    result = db.execute(query)
    
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    
    # Convertir a modelos Pydantic
    items = []
    for row in result:
        # Parsear campos JSON si vienen como string
        for json_field in ['tags', 'pros', 'cons', 'authors']:
            if json_field in row and isinstance(row[json_field], str):
                row[json_field] = json.loads(row[json_field])
        items.append(DataExtractionResponse(**row))
    
    return DataExtractionListResponse(total=total, items=items)

@app.get("/api/data-extractions/{extraction_id}", response_model=DataExtractionResponse)
def get_data_extraction(extraction_id: UUID):
    """Obtiene un data extraction por ID"""
    query = f"SELECT * FROM data_extraction_responses WHERE id = '{extraction_id}'"
    result = db.execute(query)
    
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    
    if not result:
        raise HTTPException(status_code=404, detail="Data extraction not found")
    
    row = result[0]
    for json_field in ['tags', 'pros', 'cons', 'authors']:
        if json_field in row and isinstance(row[json_field], str):
            row[json_field] = json.loads(row[json_field])
    
    return DataExtractionResponse(**row)

@app.post("/api/data-extractions", response_model=DataExtractionResponse, status_code=201)
def create_data_extraction(extraction: DataExtractionResponse):
    """Crea un nuevo data extraction"""
    
    # Convertir a dict y preparar para inserción
    data = extraction.model_dump()
    
    # Construir query INSERT con valores correctamente escapados
    columns = []
    values = []
    
    for key, value in data.items():
        columns.append(key)
        
        if value is None:
            values.append("NULL")
        elif isinstance(value, (list, dict)):
            # Escapar comillas simples en JSON y convertir a tipo JSON
            json_str = json.dumps(value).replace("'", "''")
            values.append(f"'{json_str}'::JSON")
        elif isinstance(value, str):
            # Escapar comillas simples en strings
            escaped_value = value.replace("'", "''")
            values.append(f"'{escaped_value}'")
        elif isinstance(value, UUID):
            # UUIDs necesitan comillas
            values.append(f"'{value}'")
        elif isinstance(value, date):
            # Dates necesitan comillas y formato ISO
            values.append(f"'{value.isoformat()}'::DATE")
        else:
            values.append(str(value))
    
    columns_str = ", ".join(columns)
    values_str = ", ".join(values)
    
    query = f"""
        INSERT INTO data_extraction_responses ({columns_str})
        VALUES ({values_str})
        RETURNING *
    """
    
    result = db.execute(query)
    
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    
    row = result[0]
    for json_field in ['tags', 'pros', 'cons', 'authors']:
        if json_field in row and isinstance(row[json_field], str):
            row[json_field] = json.loads(row[json_field])
    
    return DataExtractionResponse(**row)

# ============================================================
# ENDPOINTS ESPECÍFICOS PARA TRADE IDEAS
# ============================================================

@app.get("/api/trade-ideas", response_model=TradeIdeaListResponse)
def get_trade_ideas(
    limit: int = Query(default=10, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    min_conviction: Optional[int] = Query(default=None, ge=1, le=10),
    recommendation: Optional[str] = None
):
    """Obtiene lista de trade ideas con paginación y filtros"""
    
    # Construir query con filtros
    where_clauses = []
    if min_conviction:
        where_clauses.append(f"conviction >= {min_conviction}")
    if recommendation:
        where_clauses.append(f"recommendation = '{recommendation}'")
    
    where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    # Obtener total
    count_query = f"SELECT COUNT(*) as total FROM trade_ideas {where_clause}"
    total_result = db.execute(count_query)
    total = total_result[0]["total"] if total_result else 0
    
    # Obtener datos
    query = f"""
        SELECT * FROM trade_ideas 
        {where_clause}
        ORDER BY conviction DESC, recommendation
        LIMIT {limit} OFFSET {offset}
    """
    result = db.execute(query)
    
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    
    # Convertir a modelos Pydantic
    items = []
    for row in result:
        for json_field in ['pros', 'cons']:
            if json_field in row and isinstance(row[json_field], str):
                row[json_field] = json.loads(row[json_field])
        items.append(TradeIdea(**row))
    
    return TradeIdeaListResponse(total=total, items=items)

@app.get("/api/trade-ideas/{idea_id}", response_model=TradeIdea)
def get_trade_idea(idea_id: UUID):
    """Obtiene una trade idea por ID"""
    query = f"SELECT * FROM trade_ideas WHERE id = '{idea_id}'"
    result = db.execute(query)
    
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    
    if not result:
        raise HTTPException(status_code=404, detail="Trade idea not found")
    
    row = result[0]
    for json_field in ['pros', 'cons']:
        if json_field in row and isinstance(row[json_field], str):
            row[json_field] = json.loads(row[json_field])
    
    return TradeIdea(**row)

@app.get("/api/data-extractions/{extraction_id}/trade-ideas", response_model=TradeIdeaListResponse)
def get_trade_ideas_by_extraction(extraction_id: UUID):
    """Obtiene todas las trade ideas de un data extraction"""
    query = f"""
        SELECT * FROM trade_ideas 
        WHERE data_extraction_id = '{extraction_id}'
        ORDER BY conviction DESC
    """
    result = db.execute(query)
    
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    
    items = []
    for row in result:
        for json_field in ['pros', 'cons']:
            if json_field in row and isinstance(row[json_field], str):
                row[json_field] = json.loads(row[json_field])
        items.append(TradeIdea(**row))
    
    return TradeIdeaListResponse(total=len(items), items=items)

@app.post("/api/trade-ideas", response_model=TradeIdea, status_code=201)
def create_trade_idea(idea: TradeIdea):
    """Crea una nueva trade idea"""
    data = idea.model_dump()
    
    columns = []
    values = []
    
    for key, value in data.items():
        columns.append(key)
        
        if value is None:
            values.append("NULL")
        elif isinstance(value, (list, dict)):
            json_str = json.dumps(value).replace("'", "''")
            values.append(f"'{json_str}'::JSON")
        elif isinstance(value, str):
            escaped_value = value.replace("'", "''")
            values.append(f"'{escaped_value}'")
        elif isinstance(value, UUID):
            values.append(f"'{value}'")
        else:
            values.append(str(value))
    
    columns_str = ", ".join(columns)
    values_str = ", ".join(values)
    
    query = f"""
        INSERT INTO trade_ideas ({columns_str})
        VALUES ({values_str})
        RETURNING *
    """
    
    result = db.execute(query)
    
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    
    row = result[0]
    for json_field in ['pros', 'cons']:
        if json_field in row and isinstance(row[json_field], str):
            row[json_field] = json.loads(row[json_field])
    
    return TradeIdea(**row)

# ============================================================
# ENDPOINTS DE ANÁLISIS
# ============================================================

@app.get("/api/analytics/conviction-distribution")
def get_conviction_distribution():
    """Obtiene la distribución de convicciones"""
    query = """
        SELECT conviction, COUNT(*) as count
        FROM trade_ideas
        GROUP BY conviction
        ORDER BY conviction
    """
    result = db.execute(query)
    
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    
    return {"distribution": result}

@app.get("/api/analytics/top-tags")
def get_top_tags(limit: int = Query(default=10, ge=1, le=50)):
    """Obtiene los tags más usados"""
    # DuckDB almacena tags como JSON, necesitamos convertir a ARRAY primero
    query = f"""
        WITH exploded_tags AS (
            SELECT UNNEST(CAST(tags AS VARCHAR[])) as tag
            FROM data_extraction_responses
            WHERE tags IS NOT NULL
        )
        SELECT tag, COUNT(*) as count
        FROM exploded_tags
        WHERE tag IS NOT NULL AND tag != ''
        GROUP BY tag
        ORDER BY count DESC
        LIMIT {limit}
    """
    result = db.execute(query)
    
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    
    return {"top_tags": result}

# ============================================================
# ENDPOINTS DEL DASHBOARD
# ============================================================

# Agregar modelo para el request del dashboard
class DashboardQueryRequest(BaseModel):
    """Request para consulta del dashboard"""
    tags: Optional[List[str]] = None
    startDate: Optional[str] = None  # Formato: YYYY-MM-DD
    endDate: Optional[str] = None    # Formato: YYYY-MM-DD

class DataExtractionWithTradeIdeas(BaseModel):
    """Data extraction con sus trade ideas asociadas"""
    extraction: DataExtractionResponse
    trade_ideas: List[TradeIdea]

class DashboardResponse(BaseModel):
    """Respuesta del dashboard con métricas"""
    total_extractions: int
    total_trade_ideas: int
    date_range: dict
    tags_filter: Optional[List[str]]
    results: List[DataExtractionWithTradeIdeas]

@app.post("/api/dashboard", response_model=DashboardResponse)
async def get_dashboard_data(request: DashboardQueryRequest):
    """
    Obtiene data extractions con sus trade ideas filtrados por tags y fechas
    
    Parámetros:
    - tags: Lista de tags para filtrar (OR logic)
    - startDate: Fecha inicial (YYYY-MM-DD)
    - endDate: Fecha final (YYYY-MM-DD)
    """
    
    try:
        # Log de entrada para debugging
        print(f"📊 Dashboard request received:")
        print(f"   Tags: {request.tags}")
        print(f"   Start Date: {request.startDate}")
        print(f"   End Date: {request.endDate}")
        
        # Construir cláusula WHERE para filtros
        where_clauses = []
        
        # Filtro por tags (si al menos un tag coincide)
        if request.tags and len(request.tags) > 0:
            # Para JSON en DuckDB, necesitamos usar json_contains o CAST
            # Opción 1: Convertir JSON a VARCHAR[] y usar list_contains
            tag_conditions = []
            for tag in request.tags:
                # Escapar comillas en el tag
                escaped_tag = tag.replace("'", "''")
                tag_conditions.append(f"list_contains(CAST(tags AS VARCHAR[]), '{escaped_tag}')")
            
            where_clauses.append(f"({' OR '.join(tag_conditions)})")
            print(f"   📌 Tag filter: {tag_conditions}")
        
        # Filtro por fecha inicial
        if request.startDate:
            where_clauses.append(f"date >= '{request.startDate}'::DATE")
            print(f"   📅 Start date filter: {request.startDate}")
        
        # Filtro por fecha final
        if request.endDate:
            where_clauses.append(f"date <= '{request.endDate}'::DATE")
            print(f"   📅 End date filter: {request.endDate}")
        
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        # Query para obtener data extractions
        extractions_query = f"""
            SELECT * FROM data_extraction_responses
            {where_clause}
            ORDER BY date DESC, title
        """
        
        print(f"   🔍 Query: {extractions_query}")
        
        try:
            extractions_result = db.execute(extractions_query)
        except Exception as db_error:
            print(f"   ❌ Database error on extractions query: {str(db_error)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Error executing extractions query: {str(db_error)}"
            )
        
        if isinstance(extractions_result, dict) and "error" in extractions_result:
            print(f"   ❌ Query returned error: {extractions_result['error']}")
            raise HTTPException(
                status_code=400, 
                detail=f"Database query error: {extractions_result['error']}"
            )
        
        print(f"   ✅ Found {len(extractions_result) if extractions_result else 0} extractions")
        
        # Procesar cada extraction y obtener sus trade ideas
        results = []
        total_trade_ideas = 0
        
        for idx, extraction_row in enumerate(extractions_result):
            try:
                print(f"   📄 Processing extraction {idx + 1}...")
                
                # Parsear campos JSON del extraction
                for json_field in ['tags', 'pros', 'cons', 'authors']:
                    if json_field in extraction_row and isinstance(extraction_row[json_field], str):
                        try:
                            extraction_row[json_field] = json.loads(extraction_row[json_field])
                        except json.JSONDecodeError as json_error:
                            print(f"      ⚠ JSON parse error for field '{json_field}': {json_error}")
                            extraction_row[json_field] = []
                
                extraction = DataExtractionResponse(**extraction_row)
                print(f"      ✓ Extraction created: {extraction.title}")
                
                # Obtener trade ideas asociadas
                trade_ideas_query = f"""
                    SELECT * FROM trade_ideas
                    WHERE data_extraction_id = '{extraction.id}'
                    ORDER BY conviction DESC
                """
                
                try:
                    trade_ideas_result = db.execute(trade_ideas_query)
                except Exception as trade_error:
                    print(f"      ⚠ Error fetching trade ideas: {trade_error}")
                    trade_ideas_result = []
                
                trade_ideas = []
                if trade_ideas_result and not isinstance(trade_ideas_result, dict):
                    for idea_row in trade_ideas_result:
                        try:
                            # Parsear campos JSON de trade ideas
                            for json_field in ['pros', 'cons']:
                                if json_field in idea_row and isinstance(idea_row[json_field], str):
                                    try:
                                        idea_row[json_field] = json.loads(idea_row[json_field])
                                    except json.JSONDecodeError:
                                        idea_row[json_field] = []
                            
                            trade_ideas.append(TradeIdea(**idea_row))
                        except Exception as idea_error:
                            print(f"      ⚠ Error parsing trade idea: {idea_error}")
                            continue
                    
                    total_trade_ideas += len(trade_ideas)
                    print(f"      ✓ Found {len(trade_ideas)} trade ideas")
                
                results.append(DataExtractionWithTradeIdeas(
                    extraction=extraction,
                    trade_ideas=trade_ideas
                ))
                
            except Exception as extraction_error:
                print(f"   ❌ Error processing extraction {idx + 1}: {str(extraction_error)}")
                print(f"      Raw data: {extraction_row}")
                # Continuar con la siguiente extracción en lugar de fallar completamente
                continue
        
        # Construir respuesta con métricas
        date_range = {
            "start": request.startDate if request.startDate else "No especificada",
            "end": request.endDate if request.endDate else "No especificada"
        }
        
        print(f"   ✅ Dashboard response ready:")
        print(f"      Total extractions: {len(results)}")
        print(f"      Total trade ideas: {total_trade_ideas}")
        
        return DashboardResponse(
            total_extractions=len(results),
            total_trade_ideas=total_trade_ideas,
            date_range=date_range,
            tags_filter=request.tags,
            results=results
        )
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        # Capturar cualquier otra excepción no manejada
        print(f"   ❌ Unexpected error in dashboard endpoint: {str(e)}")
        import traceback
        print(f"   Stack trace: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error in dashboard endpoint: {str(e)}"
        )

if __name__ == "__main__":
    host = os.environ.get("HOST", "localhost")
    port = int(os.environ.get("PORT", 9000))
    uvicorn.run(app, host=host, port=port)
