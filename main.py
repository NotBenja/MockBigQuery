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

if __name__ == "__main__":
    host = os.environ.get("HOST", "localhost")
    port = int(os.environ.get("PORT", 9000))
    uvicorn.run(app, host=host, port=port)
