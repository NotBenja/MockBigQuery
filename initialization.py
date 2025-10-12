import requests
import json
from pathlib import Path
from datetime import date
from typing import Dict, Any, List
import sys

BASE_URL = "http://localhost:9000"
MOCK_DATA_DIR = Path("mock_data")

def print_section(title: str):
    """Imprime una sección con formato"""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")

def check_server():
    """Verifica que el servidor esté corriendo"""
    try:
        response = requests.get(BASE_URL, timeout=2)
        if response.status_code == 200:
            print("✓ Servidor corriendo correctamente")
            return True
        else:
            print(f"❌ Servidor respondió con código {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print(f"❌ No se puede conectar al servidor en {BASE_URL}")
        print("   Por favor, ejecuta 'python main.py' en otra terminal primero")
        return False

def drop_tables():
    """Elimina las tablas existentes"""
    print_section("LIMPIANDO TABLAS EXISTENTES")
    
    tables = ["trade_ideas", "data_extraction_responses"]
    for table in tables:
        try:
            response = requests.post(
                f"{BASE_URL}/query",
                json={"sql": f"DROP TABLE IF EXISTS {table} CASCADE"}
            )
            if response.status_code == 200:
                print(f"✓ Tabla {table} eliminada")
            else:
                print(f"⚠ Error al eliminar {table}: {response.text}")
        except Exception as e:
            print(f"⚠ Error: {e}")

def create_tables():
    """Crea las tablas con el esquema V2"""
    print_section("CREANDO TABLAS")
    
    # Tabla principal: data_extraction_responses
    print("\n1. Creando tabla data_extraction_responses...")
    response = requests.post(
        f"{BASE_URL}/create_table",
        json={
            "name": "data_extraction_responses",
            "table_schema": """
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                date DATE NOT NULL,
                tags JSON,
                pros JSON,
                cons JSON,
                authors JSON
            """.strip()
        }
    )
    
    if response.status_code == 200:
        print("✓ Tabla data_extraction_responses creada")
    else:
        print(f"❌ Error: {response.text}")
        return False
    
    # Tabla relacionada: trade_ideas (sin CASCADE)
    print("\n2. Creando tabla trade_ideas...")
    response = requests.post(
        f"{BASE_URL}/create_table",
        json={
            "name": "trade_ideas",
            "table_schema": """
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                recommendation TEXT NOT NULL,
                summary TEXT NOT NULL,
                conviction INTEGER CHECK (conviction >= 1 AND conviction <= 10),
                pros JSON,
                cons JSON,
                data_extraction_id UUID REFERENCES data_extraction_responses(id)
            """.strip()
        }
    )
    
    if response.status_code == 200:
        print("✓ Tabla trade_ideas creada")
    else:
        print(f"❌ Error: {response.text}")
        return False
    
    return True

def load_json_file(filepath: Path) -> Dict[str, Any]:
    """Carga un archivo JSON"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def insert_data_extraction(summary_data: Dict[str, Any], filename: str) -> str:
    """Inserta un data extraction y retorna su UUID"""
    
    # Extraer información del nombre del archivo
    # Formato: {topic}-{model}-summary.json
    parts = filename.replace('-summary.json', '').split('-')
    topic = parts[0].title()
    model = parts[1].upper() if len(parts) > 1 else "Unknown"
    
    # Obtener y validar la fecha
    raw_date = summary_data.get("date", "")
    
    # Lista de valores inválidos de fecha
    invalid_dates = [
        "Información no disponible", 
        "Fecha no disponible", 
        "No disponible",
        "N/A",
        ""
    ]
    
    # Si la fecha es inválida o vacía, usar fecha actual
    if not raw_date or raw_date in invalid_dates:
        valid_date = date.today().isoformat()
    else:
        # Intentar parsear la fecha
        try:
            # Si ya es un string en formato ISO, usarlo
            if isinstance(raw_date, str) and len(raw_date) == 10:
                # Validar que sea una fecha válida
                date.fromisoformat(raw_date)
                valid_date = raw_date
            else:
                valid_date = date.today().isoformat()
        except (ValueError, TypeError):
            valid_date = date.today().isoformat()
    
    # Preparar datos para inserción
    data = {
        "title": f"Análisis {topic} - Modelo {model}",
        "summary": summary_data.get("summary", ""),
        "date": valid_date,
        "tags": summary_data.get("tags", []),
        "pros": [summary_data.get("pros", "")] if isinstance(summary_data.get("pros"), str) else summary_data.get("pros", []),
        "cons": [summary_data.get("cons", "")] if isinstance(summary_data.get("cons"), str) else summary_data.get("cons", []),
        "authors": summary_data.get("authors", [])
    }
    
    response = requests.post(
        f"{BASE_URL}/api/data-extractions",
        json=data
    )
    
    if response.status_code == 201:
        result = response.json()
        extraction_id = result["id"]
        print(f"  ✓ Data Extraction creado: {data['title']} (ID: {extraction_id[:8]}...)")
        return extraction_id
    else:
        print(f"  ❌ Error: {response.text}")
        print(f"  📋 Datos enviados: {json.dumps(data, indent=2, default=str)}")
        return None

def insert_trade_ideas(trade_data: Dict[str, Any], extraction_id: str, filename: str):
    """Inserta las trade ideas asociadas a un data extraction"""
    
    trade_ideas = trade_data.get("tradeIdeas", [])
    
    for idx, idea in enumerate(trade_ideas, 1):
        data = {
            "recommendation": idea.get("recommendation", ""),
            "summary": idea.get("summary", ""),
            "conviction": idea.get("conviction", 5),
            "pros": idea.get("pros", []),
            "cons": idea.get("cons", []),
            "data_extraction_id": extraction_id
        }
        
        response = requests.post(
            f"{BASE_URL}/api/trade-ideas",
            json=data
        )
        
        if response.status_code == 201:
            print(f"    ✓ Trade Idea {idx} creada (Convicción: {data['conviction']}/10)")
        else:
            print(f"    ❌ Error en Trade Idea {idx}: {response.text}")

def load_mock_data():
    """Carga todos los datos mock"""
    print_section("CARGANDO DATOS MOCK")
    
    summary_dir = MOCK_DATA_DIR / "summary"
    trade_dir = MOCK_DATA_DIR / "trade"
    
    # Obtener todos los archivos de summary
    summary_files = list(summary_dir.glob("*.json"))
    
    if not summary_files:
        print("❌ No se encontraron archivos mock en mock_data/summary/")
        return False
    
    print(f"\n📂 Encontrados {len(summary_files)} archivos de summary")
    
    loaded_count = 0
    error_count = 0
    
    for summary_file in sorted(summary_files):
        print(f"\n{'─'*70}")
        print(f"📄 Procesando: {summary_file.name}")
        
        try:
            # Cargar summary
            summary_data = load_json_file(summary_file)
            
            # Insertar data extraction
            extraction_id = insert_data_extraction(summary_data, summary_file.name)
            
            if not extraction_id:
                error_count += 1
                continue
            
            # Buscar el archivo de trade correspondiente
            trade_file = trade_dir / summary_file.name.replace("-summary.json", "-trade.json")
            
            if trade_file.exists():
                trade_data = load_json_file(trade_file)
                insert_trade_ideas(trade_data, extraction_id, trade_file.name)
            else:
                print(f"  ⚠ No se encontró archivo de trade: {trade_file.name}")
            
            loaded_count += 1
            
        except Exception as e:
            print(f"  ❌ Error procesando {summary_file.name}: {e}")
            error_count += 1
    
    print(f"\n{'='*70}")
    print(f"✓ Procesados: {loaded_count} archivos")
    if error_count > 0:
        print(f"⚠ Errores: {error_count} archivos")
    
    return True

def show_statistics():
    """Muestra estadísticas de los datos cargados"""
    print_section("ESTADÍSTICAS DE DATOS CARGADOS")
    
    # Total de data extractions
    response = requests.post(
        f"{BASE_URL}/query",
        json={"sql": "SELECT COUNT(*) as total FROM data_extraction_responses"}
    )
    if response.status_code == 200:
        total_extractions = response.json()["rows"][0]["total"]
        print(f"📊 Total Data Extractions: {total_extractions}")
    
    # Total de trade ideas
    response = requests.post(
        f"{BASE_URL}/query",
        json={"sql": "SELECT COUNT(*) as total FROM trade_ideas"}
    )
    if response.status_code == 200:
        total_ideas = response.json()["rows"][0]["total"]
        print(f"💡 Total Trade Ideas: {total_ideas}")
    
    # Distribución por convicción
    response = requests.get(f"{BASE_URL}/api/analytics/conviction-distribution")
    if response.status_code == 200:
        distribution = response.json()["distribution"]
        print(f"\n📈 Distribución por Convicción:")
        for item in distribution:
            conviction = item["conviction"]
            count = item["count"]
            bar = "█" * count
            print(f"   Convicción {conviction}: {bar} ({count})")
    
    # Top tags
    response = requests.get(f"{BASE_URL}/api/analytics/top-tags?limit=5")
    if response.status_code == 200:
        top_tags = response.json()["top_tags"]
        print(f"\n🏷️  Top 5 Tags:")
        for idx, item in enumerate(top_tags, 1):
            print(f"   {idx}. {item['tag']} ({item['count']} usos)")
    
    # Algunos ejemplos de datos
    print(f"\n📋 Ejemplos de Data Extractions:")
    response = requests.get(f"{BASE_URL}/api/data-extractions?limit=3")
    if response.status_code == 200:
        items = response.json()["items"]
        for item in items:
            print(f"   • {item['title']} ({item['date']})")
            print(f"     Tags: {', '.join(item['tags'][:3])}")

def verify_foreign_keys():
    """Verifica la integridad referencial"""
    print_section("VERIFICANDO INTEGRIDAD REFERENCIAL")
    
    # Verificar que todas las trade ideas tengan un data_extraction_id válido
    query = """
        SELECT COUNT(*) as orphans
        FROM trade_ideas ti
        LEFT JOIN data_extraction_responses der ON ti.data_extraction_id = der.id
        WHERE der.id IS NULL
    """
    
    response = requests.post(f"{BASE_URL}/query", json={"sql": query})
    if response.status_code == 200:
        orphans = response.json()["rows"][0]["orphans"]
        if orphans == 0:
            print("✓ Todas las Trade Ideas tienen un Data Extraction válido")
        else:
            print(f"⚠ Hay {orphans} Trade Ideas huérfanas (sin Data Extraction)")
    
    # Verificar constraints de convicción
    query = """
        SELECT COUNT(*) as invalid
        FROM trade_ideas
        WHERE conviction < 1 OR conviction > 10
    """
    
    response = requests.post(f"{BASE_URL}/query", json={"sql": query})
    if response.status_code == 200:
        invalid = response.json()["rows"][0]["invalid"]
        if invalid == 0:
            print("✓ Todos los valores de convicción son válidos (1-10)")
        else:
            print(f"⚠ Hay {invalid} Trade Ideas con convicción inválida")

def main():
    """Función principal"""
    print("╔" + "═"*68 + "╗")
    print("║" + " "*15 + "INICIALIZACIÓN DE BASE DE DATOS" + " "*21 + "║")
    print("║" + " "*18 + "Mock BigQuery con DuckDB" + " "*25 + "║")
    print("╚" + "═"*68 + "╝")
    
    # 1. Verificar servidor
    if not check_server():
        sys.exit(1)
    
    # 2. Limpiar tablas
    drop_tables()
    
    # 3. Crear tablas
    if not create_tables():
        print("\n❌ Error al crear tablas. Abortando.")
        sys.exit(1)
    
    # 4. Cargar datos
    if not load_mock_data():
        print("\n❌ Error al cargar datos. Abortando.")
        sys.exit(1)
    
    # 5. Verificar integridad
    verify_foreign_keys()
    
    # 6. Mostrar estadísticas
    show_statistics()
    
    # 7. Mensaje final
    print_section("✅ INICIALIZACIÓN COMPLETADA")
    print(f"""
    🎉 Base de datos inicializada exitosamente!
    
    📡 Puedes acceder a la API en: {BASE_URL}
    📚 Documentación interactiva: {BASE_URL}/docs
    
    Endpoints disponibles:
    • GET  {BASE_URL}/api/data-extractions
    • GET  {BASE_URL}/api/trade-ideas
    • POST {BASE_URL}/api/data-extractions
    • POST {BASE_URL}/api/trade-ideas
    
    Ejemplos de queries:
    • {BASE_URL}/api/trade-ideas?min_conviction=7
    • {BASE_URL}/api/data-extractions?tag=Equity
    • {BASE_URL}/api/analytics/conviction-distribution
    """)

if __name__ == "__main__":
    main()