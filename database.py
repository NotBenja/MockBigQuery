import duckdb
import json
from typing import Dict, Any, List, Optional

DB_PATH = "local_bigquery.db"

class DuckDBClient:
    def __init__(self, db_path: str = DB_PATH):
        self.con = duckdb.connect(db_path)
        self._init_tables()
    
    def _init_tables(self):
        """Inicializa las tablas si no existen"""
        try:
            # Tabla data_extraction_responses
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS data_extraction_responses (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    date DATE NOT NULL,
                    tags JSON,
                    pros JSON,
                    cons JSON,
                    authors JSON
                )
            """)
            
            # Tabla trade_ideas con foreign key
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS trade_ideas (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    recommendation TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    conviction INTEGER CHECK (conviction >= 1 AND conviction <= 10),
                    pros JSON,
                    cons JSON,
                    data_extraction_id UUID REFERENCES data_extraction_responses(id)
                )
            """)
            
            # Tabla de tags
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS tags (
                    id INTEGER PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    category TEXT NOT NULL
                )
            """)
            
            # Tabla intermedia para relación muchos-a-muchos
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS data_extraction_tags (
                    id INTEGER PRIMARY KEY,
                    data_extraction_id UUID REFERENCES data_extraction_responses(id),
                    tag_id INTEGER REFERENCES tags(id),
                    UNIQUE(data_extraction_id, tag_id)
                )
            """)
            
            # Crear secuencias para IDs autoincrement
            self.con.execute("""
                CREATE SEQUENCE IF NOT EXISTS tags_id_seq START 1
            """)
            
            self.con.execute("""
                CREATE SEQUENCE IF NOT EXISTS data_extraction_tags_id_seq START 1
            """)
            
        except Exception as e:
            print(f"⚠️  Tables might already exist: {e}")
    
    def execute(self, query: str) -> List[Dict[str, Any]]:
        """Ejecuta una query y retorna resultados como lista de diccionarios"""
        try:
            result = self.con.execute(query)
            try:
                return result.fetchdf().to_dict(orient="records")
            except:
                return []
        except Exception as e:
            return {"error": str(e)}
    
    def insert_data_extraction(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Inserta un data extraction y retorna el registro creado"""
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
            else:
                values.append(f"'{value}'")
        
        columns_str = ", ".join(columns)
        values_str = ", ".join(values)
        
        query = f"""
            INSERT INTO data_extraction_responses ({columns_str})
            VALUES ({values_str})
            RETURNING *
        """
        
        result = self.execute(query)
        
        if isinstance(result, dict) and "error" in result:
            raise Exception(result["error"])
        
        return result[0] if result else {}
    
    def insert_trade_idea(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Inserta una trade idea y retorna el registro creado"""
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
            else:
                values.append(f"'{value}'")
        
        columns_str = ", ".join(columns)
        values_str = ", ".join(values)
        
        query = f"""
            INSERT INTO trade_ideas ({columns_str})
            VALUES ({values_str})
            RETURNING *
        """
        
        result = self.execute(query)
        
        if isinstance(result, dict) and "error" in result:
            raise Exception(result["error"])
        
        return result[0] if result else {}
    
    def get_data_extractions(
        self, 
        tags: List[str] = None, 
        start_date: str = None, 
        end_date: str = None
    ) -> List[Dict[str, Any]]:
        """Consulta data extractions con filtros opcionales"""
        where_clauses = []
        
        # Filtro por tags (OR logic)
        if tags and len(tags) > 0:
            tag_conditions = []
            for tag in tags:
                escaped_tag = tag.replace("'", "''")
                tag_conditions.append(f"list_contains(CAST(tags AS VARCHAR[]), '{escaped_tag}')")
            where_clauses.append(f"({' OR '.join(tag_conditions)})")
        
        # Filtro por fechas
        if start_date:
            where_clauses.append(f"date >= '{start_date}'::DATE")
        if end_date:
            where_clauses.append(f"date <= '{end_date}'::DATE")
        
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        query = f"""
            SELECT * FROM data_extraction_responses
            {where_clause}
            ORDER BY date DESC, title
        """
        
        result = self.execute(query)
        
        if isinstance(result, dict) and "error" in result:
            raise Exception(result["error"])
        
        return result
    
    def get_trade_ideas_by_extraction(self, extraction_id: str) -> List[Dict[str, Any]]:
        """Obtiene todas las trade ideas de un data extraction"""
        query = f"""
            SELECT * FROM trade_ideas
            WHERE data_extraction_id = '{extraction_id}'
            ORDER BY conviction DESC
        """
        
        result = self.execute(query)
        
        if isinstance(result, dict) and "error" in result:
            raise Exception(result["error"])
        
        return result
    
    def load_tags_from_json(self, json_path: str) -> int:
        """Carga tags desde un archivo JSON"""
        with open(json_path, 'r', encoding='utf-8') as f:
            tags_data = json.load(f)
        
        # Limpiar tabla de tags existentes
        self.con.execute("DELETE FROM data_extraction_tags")
        self.con.execute("DELETE FROM tags")
        
        tags_inserted = 0
        seen_tags = set()  # Para evitar duplicados
        duplicates_skipped = []
        
        for category, tag_names in tags_data.items():
            for tag_name in tag_names:
                # Si el tag ya fue insertado, saltarlo
                if tag_name in seen_tags:
                    duplicates_skipped.append(f"{tag_name} (en {category})")
                    continue
                
                seen_tags.add(tag_name)
                escaped_name = tag_name.replace("'", "''")
                escaped_category = category.replace("'", "''")
                
                self.con.execute(f"""
                    INSERT INTO tags (id, name, category)
                    VALUES (nextval('tags_id_seq'), '{escaped_name}', '{escaped_category}')
                """)
                tags_inserted += 1
        
        if duplicates_skipped:
            print(f"\n⚠️  Tags duplicados omitidos ({len(duplicates_skipped)}):")
            for dup in duplicates_skipped[:10]:  # Mostrar solo los primeros 10
                print(f"   - {dup}")
            if len(duplicates_skipped) > 10:
                print(f"   ... y {len(duplicates_skipped) - 10} más")
        
        return tags_inserted
    
    def link_tags_to_extraction(self, extraction_id: str, tag_names: List[str]) -> int:
        """Vincula tags a un data extraction"""
        linked_count = 0
        not_found = []
        
        for tag_name in tag_names:
            escaped_tag = tag_name.replace("'", "''")
            
            # Buscar tag
            tag_result = self.execute(f"""
                SELECT id FROM tags WHERE name = '{escaped_tag}'
            """)
            
            if tag_result and len(tag_result) > 0:
                tag_id = tag_result[0]['id']
                
                # Verificar si ya existe la relación
                existing = self.execute(f"""
                    SELECT id FROM data_extraction_tags 
                    WHERE data_extraction_id = '{extraction_id}' AND tag_id = {tag_id}
                """)
                
                if not existing:
                    self.con.execute(f"""
                        INSERT INTO data_extraction_tags (id, data_extraction_id, tag_id)
                        VALUES (nextval('data_extraction_tags_id_seq'), '{extraction_id}', {tag_id})
                    """)
                    linked_count += 1
            else:
                not_found.append(tag_name)
        
        if not_found:
            print(f"⚠️  Tags no encontrados en la BD: {', '.join(not_found)}")
        
        return linked_count
    
    def get_popular_tags(
        self, 
        tag_names: List[str] = None, 
        start_date: str = None, 
        end_date: str = None, 
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Obtiene los tags más populares según filtros"""
        where_clauses = []
        
        # Filtro por tags específicos
        if tag_names and len(tag_names) > 0:
            escaped_tags = [f"'{t.replace('\'', '\'\'')}'" for t in tag_names]
            where_clauses.append(f"t.name IN ({', '.join(escaped_tags)})")
        
        # Filtro por fechas
        if start_date:
            where_clauses.append(f"de.date >= '{start_date}'::DATE")
        if end_date:
            where_clauses.append(f"de.date <= '{end_date}'::DATE")
        
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        query = f"""
            SELECT 
                t.name,
                COUNT(DISTINCT det.id) as count
            FROM tags t
            JOIN data_extraction_tags det ON t.id = det.tag_id
            JOIN data_extraction_responses de ON det.data_extraction_id = de.id
            {where_clause}
            GROUP BY t.name
            ORDER BY count DESC
            LIMIT {limit}
        """
        
        result = self.execute(query)
        return result if result else []
    
    def get_extractions_by_country(
        self, 
        tag_names: List[str] = None, 
        start_date: str = None, 
        end_date: str = None
    ) -> List[Dict[str, Any]]:
        """Obtiene cantidad de extracciones por país"""
        where_clauses = ["t.category = 'country'"]
        
        # Filtro por tags adicionales
        if tag_names and len(tag_names) > 0:
            escaped_tags = [f"'{t.replace('\'', '\'\'')}'" for t in tag_names]
            where_clauses.append(f"""
                de.id IN (
                    SELECT det2.data_extraction_id 
                    FROM data_extraction_tags det2 
                    JOIN tags t2 ON det2.tag_id = t2.id 
                    WHERE t2.name IN ({', '.join(escaped_tags)})
                )
            """)
        
        # Filtro por fechas
        if start_date:
            where_clauses.append(f"de.date >= '{start_date}'::DATE")
        if end_date:
            where_clauses.append(f"de.date <= '{end_date}'::DATE")
        
        where_clause = "WHERE " + " AND ".join(where_clauses)
        
        query = f"""
            SELECT 
                t.name as label,
                COUNT(DISTINCT de.id) as value
            FROM tags t
            JOIN data_extraction_tags det ON t.id = det.tag_id
            JOIN data_extraction_responses de ON det.data_extraction_id = de.id
            {where_clause}
            GROUP BY t.name
            ORDER BY value DESC
        """
        
        result = self.execute(query)
        return result if result else []
    
    def get_extractions_by_sector(
        self, 
        tag_names: List[str] = None, 
        start_date: str = None, 
        end_date: str = None
    ) -> List[Dict[str, Any]]:
        """Obtiene cantidad de extracciones por sector"""
        where_clauses = ["t.category = 'sector'"]
        
        # Filtro por tags adicionales
        if tag_names and len(tag_names) > 0:
            escaped_tags = [f"'{t.replace('\'', '\'\'')}'" for t in tag_names]
            where_clauses.append(f"""
                de.id IN (
                    SELECT det2.data_extraction_id 
                    FROM data_extraction_tags det2 
                    JOIN tags t2 ON det2.tag_id = t2.id 
                    WHERE t2.name IN ({', '.join(escaped_tags)})
                )
            """)
        
        # Filtro por fechas
        if start_date:
            where_clauses.append(f"de.date >= '{start_date}'::DATE")
        if end_date:
            where_clauses.append(f"de.date <= '{end_date}'::DATE")
        
        where_clause = "WHERE " + " AND ".join(where_clauses)
        
        query = f"""
            SELECT 
                t.name as label,
                COUNT(DISTINCT de.id) as value
            FROM tags t
            JOIN data_extraction_tags det ON t.id = det.tag_id
            JOIN data_extraction_responses de ON det.data_extraction_id = de.id
            {where_clause}
            GROUP BY t.name
            ORDER BY value DESC
        """
        
        result = self.execute(query)
        return result if result else []
    
    def drop_tables(self):
        """Elimina todas las tablas (útil para testing)"""
        self.con.execute("DROP TABLE IF EXISTS data_extraction_tags")
        self.con.execute("DROP TABLE IF EXISTS trade_ideas")
        self.con.execute("DROP TABLE IF EXISTS tags")
        self.con.execute("DROP TABLE IF EXISTS data_extraction_responses")
        self.con.execute("DROP SEQUENCE IF EXISTS tags_id_seq")
        self.con.execute("DROP SEQUENCE IF EXISTS data_extraction_tags_id_seq")