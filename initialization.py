import requests
import json
from pathlib import Path
from datetime import date
from typing import Dict, Any, List
import sys
from database import DuckDBClient
import time

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
        print(f"⚠️  Servidor no está corriendo (esto es correcto para la inicialización)")
        return False

def drop_and_create_tables():
    """Elimina y recrea las tablas usando DuckDBClient"""
    print_section("LIMPIANDO Y RECREANDO TABLAS")
    
    try:
        db = DuckDBClient()
        
        # Eliminar tablas en orden inverso (por foreign keys)
        print("🗑️  Eliminando tablas existentes...")
        db.drop_tables()
        print("✓ Tablas eliminadas")
        
        # Recrear tablas
        print("\n🔨 Creando tablas nuevas...")
        db._init_tables()
        print("✓ Tablas creadas exitosamente")
        
        # Cerrar conexión
        db.con.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False

def load_json_file(filepath: Path) -> Dict[str, Any]:
    """Carga un archivo JSON"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_mock_data_direct():
    """Carga todos los datos mock DIRECTAMENTE a la BD (sin API)"""
    print_section("CARGANDO DATOS MOCK")
    
    summary_dir = MOCK_DATA_DIR / "summary"
    trade_dir = MOCK_DATA_DIR / "trade"
    
    summary_files = list(summary_dir.glob("*.json"))
    
    if not summary_files:
        print("❌ No se encontraron archivos mock en mock_data/summary/")
        return False
    
    print(f"\n📂 Encontrados {len(summary_files)} archivos de summary")
    
    # Conectar a la BD directamente
    db = DuckDBClient()
    
    loaded_count = 0
    error_count = 0
    
    for summary_file in sorted(summary_files):
        print(f"\n{'─'*70}")
        print(f"📄 Procesando: {summary_file.name}")
        
        try:
            # Cargar summary
            summary_data = load_json_file(summary_file)
            
            # Extraer información del nombre del archivo
            parts = summary_file.name.replace('-summary.json', '').split('-')
            topic = parts[0].title()
            model = parts[1].upper() if len(parts) > 1 else "Unknown"
            
            # Validar fecha
            raw_date = summary_data.get("date", "")
            invalid_dates = [
                "Información no disponible", 
                "Fecha no disponible", 
                "No disponible",
                "N/A",
                ""
            ]
            
            if not raw_date or raw_date in invalid_dates:
                valid_date = date.today().isoformat()
            else:
                try:
                    if isinstance(raw_date, str) and len(raw_date) == 10:
                        date.fromisoformat(raw_date)
                        valid_date = raw_date
                    else:
                        valid_date = date.today().isoformat()
                except (ValueError, TypeError):
                    valid_date = date.today().isoformat()
            
            # Preparar datos
            extraction_data = {
                "title": f"Análisis {topic} - Modelo {model}",
                "summary": summary_data.get("summary", ""),
                "date": valid_date,
                "tags": summary_data.get("tags", []),
                "pros": [summary_data.get("pros", "")] if isinstance(summary_data.get("pros"), str) else summary_data.get("pros", []),
                "cons": [summary_data.get("cons", "")] if isinstance(summary_data.get("cons"), str) else summary_data.get("cons", []),
                "authors": summary_data.get("authors", [])
            }
            
            # Insertar data extraction
            extraction_result = db.insert_data_extraction(extraction_data)
            extraction_id = extraction_result["id"]
            print(f"  ✓ Data Extraction creado: {extraction_data['title']} (ID: {extraction_id})")
            
            # Cargar trade ideas si existen
            trade_file = trade_dir / summary_file.name.replace("-summary.json", "-trade.json")
            
            if trade_file.exists():
                trade_data = load_json_file(trade_file)
                trade_ideas = trade_data.get("tradeIdeas", [])
                
                for idx, idea in enumerate(trade_ideas, 1):
                    idea_data = {
                        "recommendation": idea.get("recommendation", ""),
                        "summary": idea.get("summary", ""),
                        "conviction": idea.get("conviction", 5),
                        "pros": idea.get("pros", []),
                        "cons": idea.get("cons", []),
                        "data_extraction_id": extraction_id
                    }
                    
                    db.insert_trade_idea(idea_data)
                    print(f"    ✓ Trade Idea {idx} creada (Convicción: {idea_data['conviction']}/10)")
            else:
                print(f"  ⚠ No se encontró archivo de trade: {trade_file.name}")
            
            loaded_count += 1
            
        except Exception as e:
            print(f"  ❌ Error procesando {summary_file.name}: {e}")
            import traceback
            print(traceback.format_exc())
            error_count += 1
    
    # Cerrar conexión
    db.con.close()
    
    print(f"\n{'='*70}")
    print(f"✓ Procesados: {loaded_count} archivos")
    if error_count > 0:
        print(f"⚠ Errores: {error_count} archivos")
    
    return True

def show_statistics():
    """Muestra estadísticas de los datos cargados usando DuckDBClient"""
    print_section("ESTADÍSTICAS DE DATOS CARGADOS")
    
    try:
        db = DuckDBClient(read_only=True)
        
        # Total de data extractions
        result = db.execute("SELECT COUNT(*) as total FROM data_extraction_responses")
        total_extractions = result[0]["total"] if result else 0
        print(f"📊 Total Data Extractions: {total_extractions}")
        
        # Total de trade ideas
        result = db.execute("SELECT COUNT(*) as total FROM trade_ideas")
        total_ideas = result[0]["total"] if result else 0
        print(f"💡 Total Trade Ideas: {total_ideas}")
        
        # Distribución por convicción
        result = db.execute("""
            SELECT conviction, COUNT(*) as count
            FROM trade_ideas
            GROUP BY conviction
            ORDER BY conviction
        """)
        
        if result:
            print(f"\n📈 Distribución por Convicción:")
            for item in result:
                conviction = item["conviction"]
                count = item["count"]
                bar = "█" * count
                print(f"   Convicción {conviction}: {bar} ({count})")
        
        # Algunos ejemplos de datos
        print(f"\n📋 Ejemplos de Data Extractions:")
        result = db.execute("SELECT title, date FROM data_extraction_responses LIMIT 3")
        if result:
            for item in result:
                print(f"   • {item['title']} ({item['date']})")
        
        db.con.close()
        
    except Exception as e:
        print(f"⚠️  Error al obtener estadísticas: {str(e)}")

def verify_foreign_keys():
    """Verifica la integridad referencial"""
    print_section("VERIFICANDO INTEGRIDAD REFERENCIAL")
    
    try:
        db = DuckDBClient(read_only=True)
        
        # Verificar que todas las trade ideas tengan un data_extraction_id válido
        query = """
            SELECT COUNT(*) as orphans
            FROM trade_ideas ti
            LEFT JOIN data_extraction_responses der ON ti.data_extraction_id = der.id
            WHERE der.id IS NULL
        """
        
        result = db.execute(query)
        if result:
            orphans = result[0]["orphans"]
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
        
        result = db.execute(query)
        if result:
            invalid = result[0]["invalid"]
            if invalid == 0:
                print("✓ Todos los valores de convicción son válidos (1-10)")
            else:
                print(f"⚠ Hay {invalid} Trade Ideas con convicción inválida")
        
        db.con.close()
                
    except Exception as e:
        print(f"⚠️  Error al verificar integridad: {str(e)}")

def main():
    """Función principal"""
    print("╔" + "═"*68 + "╗")
    print("║" + " "*15 + "INICIALIZACIÓN DE BASE DE DATOS" + " "*21 + "║")
    print("║" + " "*18 + "Mock BigQuery con DuckDB" + " "*25 + "║")
    print("╚" + "═"*68 + "╝")
    
    # 1. Verificar si el servidor está corriendo
    server_running = check_server()
    
    if server_running:
        print("\n⚠️  ADVERTENCIA: El servidor está corriendo.")
        print("   Por favor, detén el servidor (Ctrl+C en la terminal de main.py)")
        print("   antes de ejecutar la inicialización.\n")
        response = input("¿Detuviste el servidor? (s/n): ")
        if response.lower() != 's':
            print("❌ Inicialización cancelada")
            sys.exit(1)
    
    # 2. Limpiar y recrear tablas
    if not drop_and_create_tables():
        print("\n❌ Error al crear tablas. Abortando.")
        sys.exit(1)
    
    # 3. Cargar datos DIRECTAMENTE (sin API)
    if not load_mock_data_direct():
        print("\n❌ Error al cargar datos. Abortando.")
        sys.exit(1)
    
    # 4. Verificar integridad
    verify_foreign_keys()
    
    # 5. Mostrar estadísticas
    show_statistics()
    
    # 6. Mensaje final
    print_section("✅ INICIALIZACIÓN COMPLETADA")
    print(f"""
    🎉 Base de datos inicializada exitosamente!
    
    Ahora puedes iniciar el servidor:
    
        python main.py
    
    Luego accede a:
    📡 API: {BASE_URL}
    📚 Documentación: {BASE_URL}/docs
    
    Endpoints disponibles:
    • GET  {BASE_URL}/api/data-extractions
    • POST {BASE_URL}/api/data-extractions
    • GET  {BASE_URL}/api/data-extractions/{{id}}/trade-ideas
    • POST {BASE_URL}/api/trade-ideas
    • POST {BASE_URL}/api/dashboard
    • GET  {BASE_URL}/api/tags
    """)

if __name__ == "__main__":
    main()