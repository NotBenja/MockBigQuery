from pydantic import BaseModel
from typing import List, Dict, Any
 
class QueryRequest(BaseModel):
    sql: str
 
class InsertRequest(BaseModel):
    table: str
    data: List[Dict[str, Any]]
 
class CreateTableRequest(BaseModel):
    name: str
    table_schema: str  # Ejemplo: "id INTEGER, nombre TEXT, edad INTEGER"