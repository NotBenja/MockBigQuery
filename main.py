import uvicorn
from fastapi import FastAPI, HTTPException
from models import QueryRequest, InsertRequest, CreateTableRequest
from database import DuckDBClient
import pandas as pd
import os
 
app = FastAPI(title="Mock BigQuery Service (DuckDB)")
db = DuckDBClient()
 
@app.get("/")
def root():
    return {"message": "Mock BigQuery is running"}
 
@app.post("/query")
def run_query(request: QueryRequest):
    result = db.execute(request.sql)
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return {"rows": result}
 
@app.post("/insert")
def insert_rows(request: InsertRequest):
    df = pd.DataFrame(request.data)
    try:
        # El id_field ya tiene valor por defecto "id" en el modelo
        id_field = request.id_field or "id"
        db.insert_dataframe(request.table, df, id_field)
        return {
            "status": "inserted", 
            "rows": len(df),
            "message": f"Se insertaron {len(df)} filas exitosamente"
        }
    except ValueError as ve:
        raise HTTPException(status_code=409, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
 
@app.post("/create_table")
def create_table(request: CreateTableRequest):
    # Verificar si la tabla ya existe
    if db.table_exists(request.name):
        raise HTTPException(
            status_code=409, 
            detail=f"La tabla '{request.name}' ya existe"
        )
    
    # Construir query con PRIMARY KEY si se especifica
    if request.primary_key:
        query = f"CREATE TABLE {request.name} ({request.table_schema}, PRIMARY KEY ({request.primary_key}))"
    else:
        query = f"CREATE TABLE IF NOT EXISTS {request.name} ({request.table_schema})"
    
    result = db.execute(query)
    
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    
    return {
        "status": "created", 
        "table": request.name,
        "schema": request.table_schema
    }

if __name__ == "__main__":
    host = os.environ.get("HOST", "localhost")
    port = int(os.environ.get("PORT", 9000))
    uvicorn.run(app, host=host, port=port)