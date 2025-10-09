from pydantic import BaseModel
from typing import List, Dict, Any, Optional
 
class QueryRequest(BaseModel):
    sql: str
 
class InsertRequest(BaseModel):
    table: str
    data: List[Dict[str, Any]]
    id_field: Optional[str] = "id"  # Campo que se usará como ID
 
class CreateTableRequest(BaseModel):
    name: str
    table_schema: str  # Ejemplo: "id INTEGER, nombre TEXT, edad INTEGER"
    primary_key: Optional[str] = None  # Campo que será la clave primaria