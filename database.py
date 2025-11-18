import duckdb
import json
from typing import List, Optional, Dict, Any
from pathlib import Path
from datetime import datetime

class DuckDBClient:
    """Cliente DuckDB con nueva estructura consolidada"""
    
    def __init__(self, db_path: str = "local_bigquery.db"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._initialize_database()
    
    def _initialize_database(self):
        """Crea tablas si no existen"""
        
        # 🔥 FORZAR ELIMINACIÓN DE TABLAS ANTIGUAS (v2)
        try:
            self.conn.execute("DROP TABLE IF EXISTS data_extraction_tags CASCADE")
            self.conn.execute("DROP TABLE IF EXISTS trade_ideas CASCADE")
            self.conn.execute("DROP TABLE IF EXISTS data_extraction_responses CASCADE")
            print("🗑️  Tablas v2 eliminadas")
        except Exception as e:
            print(f"⚠️  Advertencia al limpiar tablas antiguas: {str(e)}")
        
        # Tabla de tags (se mantiene separada)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tags (
                id UUID PRIMARY KEY,
                name VARCHAR NOT NULL,
                category VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Índices para tags
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tags_category 
            ON tags(category)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tags_name 
            ON tags(name)
        """)
        
        # Nueva tabla consolidada - research_extractions
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS research_extractions (
                id UUID PRIMARY KEY,
                title VARCHAR NOT NULL,
                published_date VARCHAR,
                authors JSON,
                summary JSON,
                tags JSON,
                pros JSON,
                cons JSON,
                trade_ideas JSON,
                suggested_tags JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                deleted_at TIMESTAMP
            )
        """)
        
        # Índices para búsquedas eficientes
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_extractions_published_date 
            ON research_extractions(published_date)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_extractions_created_at 
            ON research_extractions(created_at)
        """)
        
        # Tabla de relación many-to-many: research_extractions <-> tags
        # ⚠️ DuckDB no soporta CASCADE - eliminado
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS extraction_tags (
                extraction_id UUID,
                tag_id UUID,
                tag_category VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (extraction_id, tag_id)
            )
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_extraction_tags_extraction 
            ON extraction_tags(extraction_id)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_extraction_tags_tag 
            ON extraction_tags(tag_id)
        """)
        
        print("✅ Database initialized successfully")
    
    # ============================================================
    # MÉTODOS DE INSERCIÓN
    # ============================================================
    
    def insert_extraction(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Inserta una nueva extraction con trade_ideas anidados
        
        Args:
            data: Dict con estructura ExtractionTaskResponse
        
        Returns:
            Dict con los datos insertados
        """
        try:
            # Convertir listas/objetos a JSON strings
            authors_json = json.dumps(data.get('authors', []))
            summary_json = json.dumps([s if isinstance(s, dict) else {"title": "", "body": s} 
                                       for s in data.get('summary', [])])
            tags_json = json.dumps(data.get('tags', {}))
            pros_json = json.dumps(data.get('pros', []))
            cons_json = json.dumps(data.get('cons', []))
            trade_ideas_json = json.dumps(data.get('trade_ideas', []))
            suggested_tags_json = json.dumps(data.get('suggested_tags', []))
            
            query = """
                INSERT INTO research_extractions (
                    id, title, published_date, authors, summary, 
                    tags, pros, cons, trade_ideas, suggested_tags, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            self.conn.execute(query, [
                str(data['id']),
                data['title'],
                data.get('published_date'),
                authors_json,
                summary_json,
                tags_json,
                pros_json,
                cons_json,
                trade_ideas_json,
                suggested_tags_json,
                data.get('created_at', datetime.now())
            ])
            
            # Insertar relaciones con tags
            self._insert_extraction_tag_relations(str(data['id']), data.get('tags', {}))
            
            # Recuperar el registro insertado
            result = self.conn.execute(
                "SELECT * FROM research_extractions WHERE id = ?",
                [str(data['id'])]
            ).fetchone()
            
            if result:
                return self._row_to_dict(result)
            
            return None
            
        except Exception as e:
            print(f"❌ Error inserting extraction: {str(e)}")
            raise
    
    def _insert_extraction_tag_relations(self, extraction_id: str, tags_obj: Dict[str, Any]):
        """
        Inserta relaciones extraction-tags en extraction_tags
        
        Args:
            extraction_id: UUID de la extraction
            tags_obj: Objeto Tags con counterpart y listas de tags
        """
        try:
            # 🔥 MAPEO DE CATEGORÍAS: Código -> DB
            category_mapping = {
                'asset_class': 'assetClass',
                'e_d': 'eD',
                'region': 'region',
                'country': 'country',
                'sector': 'sector',
                'trade': 'trade'
            }
            
            inserted_count = 0
            
            # Procesar cada categoría de tags
            for code_category, db_category in category_mapping.items():
                tag_names = tags_obj.get(code_category, [])
                
                for tag_name in tag_names:
                    # 🔍 Buscar tag en la categoría correcta de DB
                    tag_result = self.conn.execute("""
                        SELECT id FROM tags 
                        WHERE name = ? AND category = ?
                    """, [tag_name, db_category]).fetchone()
                    
                    if tag_result:
                        tag_id = tag_result[0]
                        
                        # Insertar relación (ignorar si ya existe)
                        try:
                            self.conn.execute("""
                                INSERT INTO extraction_tags (extraction_id, tag_id, tag_category)
                                VALUES (?, ?, ?)
                            """, [extraction_id, str(tag_id), db_category])
                            inserted_count += 1
                            print(f"      ✓ Vinculado: {tag_name} ({db_category})")
                        except:
                            pass  # Ignorar duplicados
                    else:
                        print(f"      ⚠️  Tag '{tag_name}' no encontrado en categoría '{db_category}'")
            
            # Procesar counterpart como tag especial
            counterpart = tags_obj.get('counterpart')
            if counterpart:
                # 🔥 Normalizar counterpart (quitar espacios)
                counterpart_normalized = counterpart.replace(' ', '')
                
                tag_result = self.conn.execute(
                    "SELECT id FROM tags WHERE name = ? AND category = 'counterpart'",
                    [counterpart_normalized]
                ).fetchone()
                
                if tag_result:
                    tag_id = tag_result[0]
                    try:
                        self.conn.execute("""
                            INSERT INTO extraction_tags (extraction_id, tag_id, tag_category)
                            VALUES (?, ?, 'counterpart')
                        """, [extraction_id, str(tag_id)])
                        inserted_count += 1
                        print(f"      ✓ Vinculado: {counterpart_normalized} (counterpart)")
                    except:
                        pass  # Ignorar duplicados
                else:
                    print(f"      ⚠️  Counterpart '{counterpart}' (normalizado: '{counterpart_normalized}') no encontrado")
            
            print(f"   ✅ Total tags vinculados: {inserted_count}")
            
        except Exception as e:
            print(f"⚠️ Warning inserting tag relations: {str(e)}")
            import traceback
            print(traceback.format_exc())
    
    # ============================================================
    # MÉTODOS DE CONSULTA
    # ============================================================
    
    def get_extractions(
        self, 
        tags: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Obtiene extractions con filtros opcionales
        
        Args:
            tags: Lista de nombres de tags (AND logic - debe tener TODOS)
            start_date: Fecha inicial (YYYY-MM-DD)
            end_date: Fecha final (YYYY-MM-DD)
            limit: Límite de resultados
        
        Returns:
            Lista de dicts con extractions
        """
        try:
            base_query = "SELECT * FROM research_extractions re"
            params = []
            where_clauses = []
            
            # Filtro por tags (AND logic - debe tener TODOS los tags)
            if tags and len(tags) > 0:
                # Normalizar tags (quitar espacios para counterparts)
                normalized_tags = [tag.replace(' ', '') for tag in tags]
                
                # Subconsulta: encontrar extractions que tengan TODOS los tags
                placeholders = ','.join(['?' for _ in normalized_tags])
                where_clauses.append(f"""
                    re.id IN (
                        SELECT et.extraction_id
                        FROM extraction_tags et
                        JOIN tags t ON et.tag_id = t.id
                        WHERE t.name IN ({placeholders})
                        GROUP BY et.extraction_id
                        HAVING COUNT(DISTINCT t.name) = ?
                    )
                """)
                params.extend(normalized_tags)
                params.append(len(normalized_tags))
            
            # Filtro por fechas
            if start_date:
                where_clauses.append("re.published_date >= ?")
                params.append(start_date)
            
            if end_date:
                where_clauses.append("re.published_date <= ?")
                params.append(end_date)
            
            # Construir WHERE
            if where_clauses:
                base_query += " WHERE " + " AND ".join(where_clauses)
            
            # Ordenar por fecha
            base_query += " ORDER BY re.published_date DESC, re.created_at DESC"
            
            # Límite
            if limit:
                base_query += f" LIMIT {limit}"
            
            print(f"🔍 Query: {base_query}")
            print(f"📋 Params: {params}")
            
            result = self.conn.execute(base_query, params).fetchall()
            return [self._row_to_dict(row) for row in result]
            
        except Exception as e:
            print(f"❌ Error getting extractions: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return []
    
    def get_extraction_by_id(self, extraction_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene una extraction por ID"""
        try:
            result = self.conn.execute(
                "SELECT * FROM research_extractions WHERE id = ?",
                [extraction_id]
            ).fetchone()
            
            if result:
                return self._row_to_dict(result)
            return None
            
        except Exception as e:
            print(f"❌ Error getting extraction by ID: {str(e)}")
            return None
    
    # ============================================================
    # MÉTODOS DE ACTUALIZACIÓN
    # ============================================================

    def update_extraction_deleted_at(self, extraction_id: str, deleted_at: Optional[str]):
        query = """
            UPDATE research_extractions
            SET deleted_at = ?
            WHERE id = ?
        """

        try:
            self.conn.execute(query, [deleted_at, extraction_id])
            return True
        except Exception as e:
            print("DB error updating deleted_at:", e)
            return False
    
    # ============================================================
    # MÉTODOS DE ESTADÍSTICAS
    # ============================================================
    
    def get_popular_tags(
        self,
        tag_names: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Obtiene tags más populares (con AND logic si se filtran tags base)"""
        try:
            query = """
                SELECT 
                    t.name,
                    t.category,
                    COUNT(DISTINCT et.extraction_id) as usage_count
                FROM tags t
                JOIN extraction_tags et ON t.id = et.tag_id
                JOIN research_extractions re ON et.extraction_id = re.id
            """
            
            params = []
            where_clauses = []
            
            # Si se filtran por tags base, aplicar AND logic (normalizar)
            if tag_names and len(tag_names) > 0:
                normalized_tags = [tag.replace(' ', '') for tag in tag_names]
                placeholders = ','.join(['?' for _ in normalized_tags])
                where_clauses.append(f"""
                    re.id IN (
                        SELECT extraction_id 
                        FROM extraction_tags et2 
                        JOIN tags t2 ON et2.tag_id = t2.id 
                        WHERE t2.name IN ({placeholders})
                        GROUP BY extraction_id
                        HAVING COUNT(DISTINCT t2.name) = ?
                    )
                """)
                params.extend(normalized_tags)
                params.append(len(normalized_tags))
            
            if start_date:
                where_clauses.append("re.published_date >= ?")
                params.append(start_date)
            
            if end_date:
                where_clauses.append("re.published_date <= ?")
                params.append(end_date)
            
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            
            query += """
                GROUP BY t.name, t.category
                ORDER BY usage_count DESC
                LIMIT ?
            """
            params.append(limit)
            
            result = self.conn.execute(query, params).fetchall()
            return [self._row_to_dict(row) for row in result]
            
        except Exception as e:
            print(f"❌ Error getting popular tags: {str(e)}")
            return []
    
    def get_extractions_by_country(
        self,
        tag_names: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Obtiene conteo de extractions por país (con AND logic en tags base)"""
        try:
            query = """
                SELECT 
                    t.name as country,
                    COUNT(DISTINCT re.id) as count
                FROM research_extractions re
                JOIN extraction_tags et ON re.id = et.extraction_id
                JOIN tags t ON et.tag_id = t.id
                WHERE t.category = 'country'
            """
            
            params = []
            
            # Filtro AND para tags base (normalizar)
            if tag_names and len(tag_names) > 0:
                normalized_tags = [tag.replace(' ', '') for tag in tag_names]
                placeholders = ','.join(['?' for _ in normalized_tags])
                query += f""" 
                    AND re.id IN (
                        SELECT extraction_id 
                        FROM extraction_tags et2 
                        JOIN tags t2 ON et2.tag_id = t2.id 
                        WHERE t2.name IN ({placeholders})
                        GROUP BY extraction_id
                        HAVING COUNT(DISTINCT t2.name) = ?
                    )
                """
                params.extend(normalized_tags)
                params.append(len(normalized_tags))
            
            if start_date:
                query += " AND re.published_date >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND re.published_date <= ?"
                params.append(end_date)
            
            query += " GROUP BY t.name ORDER BY count DESC"
            
            result = self.conn.execute(query, params).fetchall()
            return [self._row_to_dict(row) for row in result]
            
        except Exception as e:
            print(f"❌ Error getting extractions by country: {str(e)}")
            return []
    
    def get_extractions_by_sector(
        self,
        tag_names: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Obtiene conteo de extractions por sector (con AND logic en tags base)"""
        try:
            query = """
                SELECT 
                    t.name as sector,
                    COUNT(DISTINCT re.id) as count
                FROM research_extractions re
                JOIN extraction_tags et ON re.id = et.extraction_id
                JOIN tags t ON et.tag_id = t.id
                WHERE t.category = 'sector'
            """
            
            params = []
            
            # Filtro AND para tags base (normalizar)
            if tag_names and len(tag_names) > 0:
                normalized_tags = [tag.replace(' ', '') for tag in tag_names]
                placeholders = ','.join(['?' for _ in normalized_tags])
                query += f""" 
                    AND re.id IN (
                        SELECT extraction_id 
                        FROM extraction_tags et2 
                        JOIN tags t2 ON et2.tag_id = t2.id 
                        WHERE t2.name IN ({placeholders})
                        GROUP BY extraction_id
                        HAVING COUNT(DISTINCT t2.name) = ?
                    )
                """
                params.extend(normalized_tags)
                params.append(len(normalized_tags))
            
            if start_date:
                query += " AND re.published_date >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND re.published_date <= ?"
                params.append(end_date)
            
            query += " GROUP BY t.name ORDER BY count DESC"
            
            result = self.conn.execute(query, params).fetchall()
            return [self._row_to_dict(row) for row in result]
            
        except Exception as e:
            print(f"❌ Error getting extractions by sector: {str(e)}")
            return []
    
    # ============================================================
    # MÉTODOS DE TAGS
    # ============================================================
    
    def insert_tag(self, tag_id: str, name: str, category: str) -> Optional[Dict[str, Any]]:
        """Inserta un nuevo tag"""
        try:
            query = """
                INSERT INTO tags (id, name, category, created_at)
                VALUES (?, ?, ?, ?)
            """
            
            self.conn.execute(query, [
                tag_id,
                name,
                category,
                datetime.now()
            ])
            
            # Recuperar el tag insertado
            result = self.conn.execute(
                "SELECT * FROM tags WHERE id = ?",
                [tag_id]
            ).fetchone()
            
            if result:
                return self._row_to_dict(result)
            return None
            
        except Exception as e:
            print(f"❌ Error inserting tag: {str(e)}")
            raise
    
    def get_all_tags(self) -> List[Dict[str, Any]]:
        """Obtiene todos los tags"""
        try:
            result = self.conn.execute("""
                SELECT * FROM tags
                ORDER BY category, name
            """).fetchall()
            
            return [self._row_to_dict(row) for row in result]
            
        except Exception as e:
            print(f"❌ Error getting all tags: {str(e)}")
            return []
    
    def get_tags_by_category(self, category: str) -> List[Dict[str, Any]]:
        """Obtiene tags por categoría"""
        try:
            result = self.conn.execute("""
                SELECT * FROM tags
                WHERE category = ?
                ORDER BY name
            """, [category]).fetchall()
            
            return [self._row_to_dict(row) for row in result]
            
        except Exception as e:
            print(f"❌ Error getting tags by category: {str(e)}")
            return []
    
    # ============================================================
    # MÉTODOS AUXILIARES
    # ============================================================
    
    def execute(self, query: str, params: List[Any] = None) -> List[Dict[str, Any]]:
        """Ejecuta query SQL genérico"""
        try:
            if params:
                result = self.conn.execute(query, params).fetchall()
            else:
                result = self.conn.execute(query).fetchall()
            
            return [self._row_to_dict(row) for row in result]
            
        except Exception as e:
            print(f"❌ Error executing query: {str(e)}")
            return []
    
    def _row_to_dict(self, row) -> Dict[str, Any]:
        """Convierte una fila de DuckDB a dict"""
        if not row:
            return {}
        
        # Obtener nombres de columnas
        columns = [desc[0] for desc in self.conn.description]
        
        # Crear dict
        result = dict(zip(columns, row))
        
        # Parsear campos JSON
        json_fields = ['authors', 'summary', 'tags', 'pros', 'cons', 'trade_ideas', 'suggested_tags']
        for field in json_fields:
            if field in result and isinstance(result[field], str):
                try:
                    result[field] = json.loads(result[field])
                except json.JSONDecodeError:
                    result[field] = []
        
        return result
    
    
    def close(self):
        """Cierra conexión a DB"""
        self.conn.close()
        print("✅ Database connection closed")