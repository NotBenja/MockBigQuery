import requests
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
import sys
from database import DuckDBClient
from uuid import uuid4

BASE_URL = "http://localhost:9000"
MOCK_DATA_DIR = Path("mock_data")
EXTRACTIONS_FILE = MOCK_DATA_DIR / "extractions.json"

def print_section(title: str):
    """Imprime una sección con formato"""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")

def drop_and_create_tables():
    """Elimina y recrea las tablas usando DuckDBClient"""
    print_section("LIMPIANDO Y RECREANDO TABLAS")
    
    try:
        db = DuckDBClient()
        
        # Eliminar tablas existentes
        print("🗑️  Eliminando tablas existentes...")
        try:
            db.conn.execute("DROP TABLE IF EXISTS extraction_tags CASCADE")
            db.conn.execute("DROP TABLE IF EXISTS research_extractions CASCADE")
            db.conn.execute("DROP TABLE IF EXISTS tags CASCADE")
            print("✓ Tablas eliminadas")
        except Exception as e:
            print(f"⚠️  Advertencia al eliminar tablas: {str(e)}")
        
        # Recrear tablas
        print("\n🔨 Recreando tablas nuevas...")
        db._initialize_database()
        print("✓ Tablas creadas exitosamente")
        
        db.close()
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False

def load_tags_from_json():
    """Carga tags desde tags.json y los inserta en la BD"""
    print_section("CARGANDO TAGS DESDE JSON")
    
    tags_file = MOCK_DATA_DIR / "tags" / "tags.json"
    
    if not tags_file.exists():
        print(f"⚠️  No se encontró {tags_file}")
        print("   Creando tags básicos por defecto...")
        return load_default_tags()
    
    try:
        with open(tags_file, 'r', encoding='utf-8') as f:
            tags_data = json.load(f)
        
        db = DuckDBClient()
        total_inserted = 0
        
        # Iterar por categorías
        for category, tag_list in tags_data.items():
            print(f"\n📂 Categoría: {category}")
            
            for tag_name in tag_list:
                tag_id = str(uuid4())
                try:
                    db.insert_tag(tag_id, tag_name, category)
                    total_inserted += 1
                    print(f"   ✓ {tag_name}")
                except Exception as e:
                    print(f"   ⚠️  Error al insertar {tag_name}: {str(e)}")
        
        db.close()
        print(f"\n✅ Total tags insertados: {total_inserted}")
        return True
        
    except Exception as e:
        print(f"❌ Error cargando tags: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False

def load_default_tags():
    """Carga tags por defecto si no existe tags.json"""
    db = DuckDBClient()
    
    default_tags = {
        "counterpart": ["Goldman Sachs", "JP Morgan", "Morgan Stanley", "Citigroup"],
        "asset_class": ["Equity", "Fixed Income", "Commodities", "FX", "Crypto"],
        "e_d": ["Emerging", "Developed"],
        "region": ["Asia Pacific", "Europe", "Americas", "Middle East", "Africa"],
        "country": ["Japan", "USA", "China", "Germany", "Brazil"],
        "sector": ["Technology", "Finance", "Healthcare", "Energy", "Consumer"],
        "trade": ["Long", "Short", "Neutral", "Pair Trade"]
    }
    
    total_inserted = 0
    
    for category, tag_list in default_tags.items():
        for tag_name in tag_list:
            tag_id = str(uuid4())
            try:
                db.insert_tag(tag_id, tag_name, category)
                total_inserted += 1
            except Exception as e:
                print(f"   ⚠️  Error: {str(e)}")
    
    db.close()
    print(f"✅ Tags por defecto insertados: {total_inserted}")
    return True

def load_extractions_from_dump():
    """
    Carga extractions desde mock_data/extractions.json (nuevo formato)
    """
    print_section("CARGANDO EXTRACTIONS DESDE DUMP")
    
    if not EXTRACTIONS_FILE.exists():
        print(f"❌ No se encontró el archivo: {EXTRACTIONS_FILE}")
        print("\n💡 Para generar el dump, ejecuta:")
        print("   1. Inicia el servidor: python main.py")
        print("   2. Llama al endpoint: GET http://localhost:9000/api/dumpdata")
        return False
    
    try:
        # Cargar dump
        print(f"📂 Cargando datos desde: {EXTRACTIONS_FILE}")
        with open(EXTRACTIONS_FILE, 'r', encoding='utf-8') as f:
            dump_data = json.load(f)
        
        extractions = dump_data.get("extractions", [])
        total = dump_data.get("total", len(extractions))
        version = dump_data.get("version", "unknown")
        exported_at = dump_data.get("exported_at", "unknown")
        
        print(f"📊 Información del dump:")
        print(f"   • Total extractions: {total}")
        print(f"   • Versión: {version}")
        print(f"   • Exportado el: {exported_at}")
        
        if not extractions:
            print("❌ No hay extractions en el dump")
            return False
        
        # Conectar a BD
        db = DuckDBClient()
        
        loaded_count = 0
        error_count = 0
        
        print(f"\n{'─'*70}")
        print("Insertando extractions en la base de datos...\n")
        
        for idx, extraction in enumerate(extractions, 1):
            try:
                # Insertar extraction (mantiene el ID original del dump)
                result = db.insert_extraction(extraction)
                
                if result:
                    title = extraction.get("title", "Sin título")
                    trade_count = len(extraction.get("trade_ideas", []))
                    
                    print(f"✅ [{idx}/{total}] {title[:50]}")
                    print(f"   • ID: {result['id']}")
                    print(f"   • Fecha: {extraction.get('published_date', 'N/A')}")
                    print(f"   • Trade Ideas: {trade_count}")
                    
                    loaded_count += 1
                else:
                    print(f"❌ [{idx}/{total}] Error al insertar extraction")
                    error_count += 1
                
            except Exception as e:
                print(f"❌ [{idx}/{total}] Error: {str(e)}")
                error_count += 1
        
        db.close()
        
        print(f"\n{'='*70}")
        print(f"✅ Procesados exitosamente: {loaded_count}/{total}")
        if error_count > 0:
            print(f"⚠️  Errores: {error_count}/{total}")
        
        return True
        
    except json.JSONDecodeError as e:
        print(f"❌ Error al parsear JSON: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ Error cargando extractions: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False

def show_statistics():
    """Muestra estadísticas de los datos cargados"""
    print_section("ESTADÍSTICAS DE DATOS CARGADOS")
    
    try:
        db = DuckDBClient()
        
        # Total de extractions
        result = db.execute("SELECT COUNT(*) as total FROM research_extractions", [])
        total_extractions = result[0]["total"] if result else 0
        print(f"📊 Total Research Extractions: {total_extractions}")
        
        # Total de tags
        result = db.execute("SELECT COUNT(*) as total FROM tags", [])
        total_tags = result[0]["total"] if result else 0
        print(f"🏷️  Total Tags: {total_tags}")
        
        # Total de trade ideas
        result = db.execute("""
            SELECT 
                SUM(json_array_length(trade_ideas)) as total_ideas
            FROM research_extractions
        """, [])
        
        total_ideas = result[0]["total_ideas"] if result and result[0]["total_ideas"] else 0
        print(f"💡 Total Trade Ideas (anidadas): {total_ideas}")
        
        # Trade Ideas por Extraction
        print(f"\n📈 Trade Ideas por Extraction:")
        result = db.execute("""
            SELECT 
                title,
                json_array_length(trade_ideas) as trade_count
            FROM research_extractions
            WHERE json_array_length(trade_ideas) > 0
            ORDER BY trade_count DESC
        """, [])
        
        if result:
            for item in result:
                bar = "█" * int(item['trade_count'])
                print(f"   {item['title'][:40]:40} {bar} ({item['trade_count']})")
        
        # Tags más usados
        print(f"\n🏷️  Top 5 Tags por Categoría:")
        result = db.execute("""
            SELECT 
                t.category,
                t.name,
                COUNT(DISTINCT et.extraction_id) as usage_count
            FROM tags t
            JOIN extraction_tags et ON t.id = et.tag_id
            GROUP BY t.category, t.name
            ORDER BY t.category, usage_count DESC
        """, [])
        
        if result:
            current_category = None
            count = 0
            for item in result:
                if item["category"] != current_category:
                    current_category = item["category"]
                    count = 0
                    print(f"\n   📂 {current_category}:")
                
                if count < 5:
                    print(f"      • {item['name']}: {item['usage_count']} usos")
                    count += 1
        
        db.close()
        
    except Exception as e:
        print(f"⚠️  Error al obtener estadísticas: {str(e)}")

def verify_data_integrity():
    """Verifica la integridad de los datos cargados"""
    print_section("VERIFICANDO INTEGRIDAD DE DATOS")
    
    try:
        db = DuckDBClient()
        
        # 1. Verificar counterpart
        result = db.execute("""
            SELECT COUNT(*) as count
            FROM research_extractions
            WHERE json_extract(tags, '$.counterpart') IS NULL
        """, [])
        
        no_counterpart = result[0]["count"] if result else 0
        if no_counterpart == 0:
            print("✅ Todos los extractions tienen counterpart")
        else:
            print(f"⚠️  {no_counterpart} extractions sin counterpart")
        
        # 2. Verificar vínculos extraction-tags
        result = db.execute("""
            SELECT COUNT(DISTINCT extraction_id) as linked
            FROM extraction_tags
        """, [])
        
        linked = result[0]["linked"] if result else 0
        
        result = db.execute("SELECT COUNT(*) as total FROM research_extractions", [])
        total = result[0]["total"] if result else 0
        
        if linked == total:
            print(f"✅ Todos los {total} extractions tienen tags vinculados")
        else:
            print(f"⚠️  Solo {linked}/{total} extractions tienen tags vinculados")
        
        db.close()
        
    except Exception as e:
        print(f"⚠️  Error verificando integridad: {str(e)}")

def main():
    """Función principal de inicialización"""
    print("╔" + "═"*68 + "╗")
    print("║" + " "*10 + "INICIALIZACIÓN MockBigQuery v3.0.0" + " "*24 + "║")
    print("║" + " "*15 + "Carga desde extractions.json" + " "*25 + "║")
    print("╚" + "═"*68 + "╝")
    
    print("\n⚠️  ADVERTENCIA:")
    print("   Este script eliminará TODOS los datos existentes y cargará")
    print("   las extractions desde mock_data/extractions.json\n")
    
    response = input("¿Deseas continuar? (s/n): ")
    if response.lower() != 's':
        print("❌ Inicialización cancelada")
        sys.exit(0)
    
    # 1. Limpiar y recrear tablas
    if not drop_and_create_tables():
        print("\n❌ Error al crear tablas. Abortando.")
        sys.exit(1)
    
    # 2. Cargar tags
    if not load_tags_from_json():
        print("\n⚠️  Continuando sin tags")
    
    # 3. Cargar extractions desde dump
    if not load_extractions_from_dump():
        print("\n❌ Error al cargar extractions. Abortando.")
        sys.exit(1)
    
    # 4. Verificar integridad
    verify_data_integrity()
    
    # 5. Mostrar estadísticas
    show_statistics()
    
    # 6. Mensaje final
    print_section("✅ INICIALIZACIÓN COMPLETADA")
    print(f"""
    🎉 Base de datos v3.0.0 inicializada exitosamente desde dump!
    
    📂 Archivo usado: mock_data/extractions.json
    
    Ahora puedes iniciar el servidor:
    
        python main.py
    
    Luego accede a:
    📡 API: {BASE_URL}
    📚 Documentación: {BASE_URL}/docs
    
    Para actualizar el dump:
    🔄 GET {BASE_URL}/api/dumpdata
    """)

if __name__ == "__main__":
    main()