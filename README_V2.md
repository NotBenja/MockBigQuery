# MockBigQuery V2 - Refactorización con Pydantic y Tipos Avanzados

## 🆕 Nuevas Características

### ✅ Lo que tu implementación SÍ soporta:

- **UUID**: Identificadores únicos globales
- **JSON**: Tipo nativo de DuckDB (sin necesidad de `json.dumps/loads`)
- **DATE**: Fechas nativas
- **FOREIGN KEYS**: Integridad referencial con `REFERENCES`
- **CHECK CONSTRAINTS**: Validación a nivel de base de datos
- **CASCADE**: Eliminación en cascada

## 📁 Archivos Nuevos

### `models.py`
Modelos Pydantic mejorados con:
- `DataExtractionResponse`: Modelo de dominio con validación
- `TradeIdea`: Modelo con foreign key y constraint de convicción
- Modelos de Request/Response tipados
- Ejemplos en la documentación

### `main.py`
API FastAPI mejorada con:
- Endpoints RESTful específicos (`/api/data-extractions`, `/api/trade-ideas`)
- Paginación y filtros
- Endpoints de análisis
- Validación automática con Pydantic
- Documentación automática en Swagger

### `initialization.py`
Script de inicialización que usa:
- UUID auto-generados
- JSON nativo
- Foreign keys para relacionar trade ideas con data extractions
- Tests de integridad referencial

## 🚀 Comparación: Versión Anterior vs V2

### Versión Anterior (INTEGER + TEXT)

```python
# models.py
class InsertRequest(BaseModel):
    table: str
    data: List[Dict[str, Any]]

# Tabla
CREATE TABLE data_extraction_responses (
    id INTEGER,
    tags TEXT,  # "['tag1', 'tag2']" como string
    pros TEXT   # "['pro1', 'pro2']" como string
)

# Inserción
data = {
    "tags": json.dumps(["tecnología", "IA"]),  # Manual
    "pros": json.dumps(["Pro 1", "Pro 2"])      # Manual
}

# Lectura
tags = json.loads(row["tags"])  # Manual
```

### Versión V2 (UUID + JSON nativo)

```python
# models.py
class DataExtractionResponse(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    tags: List[str]  # Lista nativa
    pros: List[str]  # Lista nativa

# Tabla
CREATE TABLE data_extraction_responses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tags JSON,  # Tipo nativo
    pros JSON   # Tipo nativo
)

# Inserción
data = {
    "tags": ["tecnología", "IA"],  # Directo
    "pros": ["Pro 1", "Pro 2"]      # Directo
}

# Lectura - DuckDB devuelve listas Python directamente
tags = row["tags"]  # Ya es una lista!
```

## 🎯 Ventajas de la Refactorización

### 1. **Tipo Seguro (Type Safety)**
```python
# ❌ Antes: Sin validación
data = {"conviction": "alto"}  # String donde debería ser int

# ✅ Ahora: Pydantic valida automáticamente
idea = TradeIdea(conviction=8)  # OK
idea = TradeIdea(conviction="alto")  # ValidationError!
idea = TradeIdea(conviction=15)  # ValidationError! (debe ser 1-10)
```

### 2. **Integridad Referencial**
```python
# ✅ Foreign Key automático
CREATE TABLE trade_ideas (
    id UUID PRIMARY KEY,
    data_extraction_id UUID REFERENCES data_extraction_responses(id)
)

# Intentar insertar con ID inexistente = Error automático
# No necesitas validar manualmente en código
```

### 3. **Queries más Potentes**
```sql
-- ✅ JOIN entre tablas relacionadas
SELECT 
    ti.recommendation,
    ti.conviction,
    der.title as source_title
FROM trade_ideas ti
JOIN data_extraction_responses der ON ti.data_extraction_id = der.id
WHERE ti.conviction >= 8

-- ✅ Trabajar con JSON directamente
SELECT 
    title,
    json_array_length(tags) as num_tags,
    json_extract(pros, '$[0]') as first_pro
FROM data_extraction_responses
WHERE json_contains(tags, '"tecnología"')
```

### 4. **UUIDs Globalmente Únicos**
```python
# ✅ Genera UUID automáticamente
INSERT INTO data_extraction_responses (title, summary, date)
VALUES ('Título', 'Resumen', '2024-01-01')
-- id se genera automáticamente como UUID

# ✅ No hay colisiones entre diferentes fuentes de datos
# ✅ Distribuibles sin conflicto
# ✅ Compatibles con sistemas externos
```

### 5. **API RESTful con Documentación Automática**
```python
# ✅ Endpoints específicos y tipados
@app.get("/api/trade-ideas", response_model=TradeIdeaListResponse)
def get_trade_ideas(
    min_conviction: int = Query(ge=1, le=10),
    limit: int = Query(default=10, le=100)
):
    ...

# Documentación automática en: http://localhost:9000/docs
```

## 📊 Esquema de Base de Datos V2

```sql
-- Tabla principal: Extracciones de datos
CREATE TABLE data_extraction_responses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    date DATE NOT NULL,
    tags JSON,
    pros JSON,
    cons JSON,
    authors JSON
);

-- Tabla relacionada: Ideas de trading
CREATE TABLE trade_ideas (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recommendation TEXT NOT NULL,
    summary TEXT NOT NULL,
    conviction INTEGER CHECK (conviction >= 1 AND conviction <= 10),
    pros JSON,
    cons JSON,
    data_extraction_id UUID REFERENCES data_extraction_responses(id) ON DELETE CASCADE
);
```

## 🏃 Cómo Usar

### Opción 1: Migrar a V2 (Recomendado)

```powershell
# 1. Iniciar el servidor V2
python main.py

# 2. En otra terminal, inicializar con datos V2
python initialization.py

# 3. Probar la API
# Abrir navegador en: http://localhost:9000/docs
```

### Opción 2: Mantener compatibilidad con ambas versiones

```powershell
# Servidor V1 en puerto 9000
python main.py

# Servidor V2 en puerto 9001
$env:PORT=9001; python main.py
```

## 📡 Ejemplos de Uso de la API V2

### Obtener Data Extractions
```bash
GET http://localhost:9000/api/data-extractions?limit=10&offset=0&tag=tecnología
```

### Obtener una Data Extraction específica
```bash
GET http://localhost:9000/api/data-extractions/{uuid}
```

### Crear nueva Data Extraction
```bash
POST http://localhost:9000/api/data-extractions
Content-Type: application/json

{
  "title": "Análisis Q1 2025",
  "summary": "Resumen del análisis...",
  "date": "2025-01-15",
  "tags": ["tecnología", "IA"],
  "pros": ["Crecimiento fuerte"],
  "cons": ["Alta volatilidad"],
  "authors": ["Juan Pérez"]
}
```

### Obtener Trade Ideas con filtros
```bash
GET http://localhost:9000/api/trade-ideas?min_conviction=7&recommendation=COMPRAR
```

### Obtener Trade Ideas de una Data Extraction
```bash
GET http://localhost:9000/api/data-extractions/{uuid}/trade-ideas
```

### Analytics: Distribución de Convicción
```bash
GET http://localhost:9000/api/analytics/conviction-distribution
```

### Analytics: Top Tags
```bash
GET http://localhost:9000/api/analytics/top-tags?limit=10
```

## 🔄 Migración desde V1 a V2

Si ya tienes datos en la versión V1:

```python
# Script de migración (crear como migration.py)
import requests

old_api = "http://localhost:9000"
new_api = "http://localhost:9001"

# 1. Exportar datos de V1
old_data = requests.post(f"{old_api}/query", 
    json={"sql": "SELECT * FROM data_extraction_responses"})

# 2. Transformar e importar a V2
for row in old_data.json()["rows"]:
    # Convertir strings JSON a listas
    row["tags"] = json.loads(row["tags"])
    row["pros"] = json.loads(row["pros"])
    row["cons"] = json.loads(row["cons"])
    row["authors"] = json.loads(row["authors"])
    
    # Crear en V2
    requests.post(f"{new_api}/api/data-extractions", json=row)
```

## 🧪 Tests

```powershell
# Ejecutar tests
python tests/run_all_tests.py
```

## 📚 Documentación Interactiva

Una vez que el servidor esté corriendo:

- **Swagger UI**: http://localhost:9000/docs
- **ReDoc**: http://localhost:9000/redoc

## 💡 Recomendaciones

1. **Usa V2 para proyectos nuevos**: Mejor tipado y más funcionalidades
2. **Migra gradualmente**: Puedes correr ambas versiones en paralelo
3. **Aprovecha los endpoints específicos**: Más semánticos que SQL directo
4. **Usa la validación de Pydantic**: Previene errores en tiempo de desarrollo
5. **Explora la documentación automática**: FastAPI genera docs completos

## 🐛 Troubleshooting

### Error: "Foreign key constraint failed"
- Verifica que el `data_extraction_id` exista antes de crear un `TradeIdea`
- Usa el endpoint GET para obtener IDs válidos

### Error: "Validation error"
- Revisa que los tipos coincidan con los modelos Pydantic
- `conviction` debe ser 1-10
- `date` debe estar en formato "YYYY-MM-DD"

### JSON no se deserializa correctamente
- En V2, DuckDB maneja JSON automáticamente
- No uses `json.dumps()` al insertar
- No uses `json.loads()` al leer

## 🎓 Recursos

- [DuckDB JSON Functions](https://duckdb.org/docs/sql/functions/json)
- [Pydantic Documentation](https://docs.pydantic.dev/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
