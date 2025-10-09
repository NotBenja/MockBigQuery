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
        db.insert_dataframe(request.table, df)
        return {"status": "inserted", "rows": len(df)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
 
@app.post("/create_table")
def create_table(request: CreateTableRequest):
    query = f"CREATE TABLE IF NOT EXISTS {request.name} ({request.table_schema})" 
    result = db.execute(query)
    return {"status": "created", "table": request.name}

if __name__ == "__main__":
    host = os.environ.get("HOST", "localhost")
    port = int(os.environ.get("PORT", 9000))
    uvicorn.run(app, host=host, port=port)