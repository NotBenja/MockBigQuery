import duckdb
import pandas as pd
from typing import List, Dict, Any, Sequence
 
DB_PATH = "local_bigquery.db"
 
class DuckDBClient:
    def __init__(self, db_path: str = DB_PATH):
        self.con = duckdb.connect(db_path)
    
    def execute(self, query: str):
        try:
            result = self.con.execute(query)
            try:
                return result.fetchdf().to_dict(orient="records")
            except duckdb.CatalogException:
                return {"status": "ok"}
        except Exception as e:
            return {"error": str(e)}
    
    def create_table(self, table_name: str, schema: str, primary_key: str = None) -> Dict[str, Any]:
        """Crea una nueva tabla en la base de datos"""
        try:
            # Construir query CREATE TABLE
            create_query = f"CREATE TABLE {table_name} ({schema})"
            
            # Ejecutar creación
            self.con.execute(create_query)
            
            return {
                "status": "success",
                "table": table_name,
                "schema": schema
            }
        except Exception as e:
            return {"error": str(e)}
    
    def table_exists(self, table_name: str) -> bool:
        """Verifica si una tabla existe en la base de datos"""
        try:
            result = self.con.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
            ).fetchall()
            return len(result) > 0
        except:
            return False
    
    def get_table_columns(self, table: str) -> List[str]:
        """Obtiene las columnas de una tabla en el orden correcto"""
        try:
            result = self.con.execute(f"PRAGMA table_info('{table}')").fetchdf()
            return result['name'].tolist()
        except:
            return []
    
    def get_max_id(self, table: str, id_field: str = "id") -> int:
        """Obtiene el ID máximo de una tabla"""
        try:
            result = self.con.execute(
                f"SELECT MAX({id_field}) as max_id FROM {table}"
            ).fetchdf()
            max_id = result['max_id'].iloc[0]
            return int(max_id) if pd.notna(max_id) else 0
        except:
            return 0
    
    def check_duplicate_rows(self, table: str, data: Sequence[Dict[str, Any]], id_field: str = "id") -> List[Any]:
        """Verifica si existen filas duplicadas basándose en el campo ID"""
        if not data:
            return []
        
        # Extraer IDs que ya existen en los datos
        existing_ids = [row.get(id_field) for row in data if row.get(id_field) is not None]
        
        if not existing_ids:
            return []
        
        # Construir consulta para verificar duplicados
        ids_str = ", ".join([str(id_val) for id_val in existing_ids])
        query = f"SELECT {id_field} FROM {table} WHERE {id_field} IN ({ids_str})"
        
        try:
            result = self.con.execute(query).fetchdf()
            return result[id_field].tolist()
        except:
            return []
    
    def check_duplicate_ids_in_batch(self, df: pd.DataFrame, id_field: str = "id") -> List[Any]:
        """Verifica si hay IDs duplicados dentro del mismo batch de datos"""
        if id_field not in df.columns:
            return []
        
        # Filtrar solo los IDs que no son nulos
        ids_with_values = df[df[id_field].notna()][id_field]
        
        # Encontrar duplicados
        duplicates = ids_with_values[ids_with_values.duplicated()].unique().tolist()
        return duplicates
    
    def insert_dataframe(self, table: str, df: pd.DataFrame, id_field: str = "id"):
        """Inserta datos con validación de duplicados y generación automática de IDs"""
        # Obtener columnas de la tabla en el orden correcto
        table_columns = self.get_table_columns(table)
        
        # Generar IDs automáticos para filas sin ID
        if id_field in table_columns:
            if id_field not in df.columns:
                # Si la columna de ID no existe en el DataFrame, crearla
                max_id = self.get_max_id(table, id_field)
                df[id_field] = range(max_id + 1, max_id + 1 + len(df))
            else:
                # Verificar duplicados dentro del batch antes de generar IDs
                batch_duplicates = self.check_duplicate_ids_in_batch(df, id_field)
                if batch_duplicates:
                    raise ValueError(f"Se encontraron IDs duplicados en el batch: {batch_duplicates}")
                
                # Calcular el máximo ID considerando:
                # 1. El ID máximo en la tabla
                # 2. Los IDs explícitos en este batch
                max_id_table = self.get_max_id(table, id_field)
                max_id_batch = df[df[id_field].notna()][id_field].max()
                
                # Usar el mayor de ambos
                if pd.notna(max_id_batch):
                    max_id = max(max_id_table, int(max_id_batch))
                else:
                    max_id = max_id_table
                
                # Si existe pero tiene valores nulos, llenarlos
                missing_ids = df[id_field].isna()
                if missing_ids.any():
                    new_ids = range(max_id + 1, max_id + 1 + missing_ids.sum())
                    df.loc[missing_ids, id_field] = list(new_ids)
        
        # Verificar duplicados con la base de datos
        data_dicts = [{str(k): v for k, v in row.items()} for row in df.to_dict('records')]
        duplicates = self.check_duplicate_rows(table, data_dicts, id_field)
        
        if duplicates:
            raise ValueError(f"Se encontraron IDs duplicados: {duplicates}")
        
        # Reordenar columnas del DataFrame para que coincidan con la tabla
        df = df[[col for col in table_columns if col in df.columns]]
        
        # Insertar datos especificando las columnas
        columns_str = ", ".join(df.columns)
        self.con.register("temp_df", df)
        self.con.execute(f"INSERT INTO {table} ({columns_str}) SELECT {columns_str} FROM temp_df")
        self.con.unregister("temp_df")