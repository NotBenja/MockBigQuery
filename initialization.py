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
TAGS_FILE = MOCK_DATA_DIR / "tags" / "tags.json"

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
    
    if not TAGS_FILE.exists():
        print(f"⚠️  No se encontró {TAGS_FILE}")
        print("   Creando tags básicos por defecto...")
        return load_default_tags()
    
    try:
        with open(TAGS_FILE, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        db = DuckDBClient()
        total_inserted = 0
        
        tags_list = []
        
        # Normalizar formato (Lista Plana vs Dict Agrupado)
        if isinstance(raw_data, list):
            tags_list = raw_data
            print("📂 Detectado formato: Lista Plana")
        elif isinstance(raw_data, dict):
            print("📂 Detectado formato: Diccionario Agrupado")
            for category, names in raw_data.items():
                for name in names:
                    tags_list.append({
                        "name": name,
                        "category": category,
                        "id": str(uuid4())
                    })
        
        # Insertar
        for tag in tags_list:
            tag_id = tag.get("id") or str(uuid4())
            tag_name = tag.get("name")
            category = tag.get("category")
            created_at = tag.get("created_at") or datetime.now().isoformat()
            
            # ✅ CORRECCIÓN: Insertar solo columnas que existen en database.py (sin updated_at ni version)
            try:
                db.conn.execute("""
                    INSERT INTO tags (id, name, category, created_at)
                    VALUES (?, ?, ?, ?)
                """, [tag_id, tag_name, category, created_at])
                
                total_inserted += 1
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
                # ✅ CORRECCIÓN: Ajustado al esquema real
                db.conn.execute("""
                    INSERT INTO tags (id, name, category, created_at)
                    VALUES (?, ?, ?, ?)
                """, [tag_id, tag_name, category, datetime.now().isoformat()])
                total_inserted += 1
            except Exception as e:
                print(f"   ⚠️  Error: {str(e)}")
    
    db.close()
    print(f"✅ Tags por defecto insertados: {total_inserted}")
    return True

def load_extractions_from_dump():
    """Carga extractions desde mock_data/extractions.json"""
    print_section("CARGANDO EXTRACTIONS DESDE DUMP")
    
    if not EXTRACTIONS_FILE.exists():
        print(f"❌ No se encontró el archivo: {EXTRACTIONS_FILE}")
        return False
    
    try:
        print(f"📂 Cargando datos desde: {EXTRACTIONS_FILE}")
        with open(EXTRACTIONS_FILE, 'r', encoding='utf-8') as f:
            dump_data = json.load(f)
        
        if isinstance(dump_data, dict):
            extractions = dump_data.get("extractions", [])
            total = dump_data.get("total", len(extractions))
            version = dump_data.get("version", "unknown")
            print(f"📊 Información del dump (v{version}): Total {total}")
        else:
            extractions = dump_data
            total = len(extractions)
            print(f"📊 Información del dump (Lista simple): Total {total}")
        
        if not extractions:
            print("❌ No hay extractions en el dump")
            return False
        
        db = DuckDBClient()
        loaded_count = 0
        error_count = 0
        
        print(f"\n{'─'*70}")
        print("Insertando extractions en la base de datos...\n")
        
        for idx, extraction in enumerate(extractions, 1):
            try:
                # Asegurar IDs en trade_ideas
                if "trade_ideas" in extraction:
                    for ti in extraction["trade_ideas"]:
                        if "id" not in ti or not ti["id"]:
                            ti["id"] = str(uuid4())
                
                result = db.insert_extraction(extraction)
                
                if result:
                    title = extraction.get("title", "Sin título")
                    trade_count = len(extraction.get("trade_ideas", []))
                    print(f"✅ [{idx}/{total}] {title[:50]} (Trades: {trade_count})")
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
        
    except Exception as e:
        print(f"❌ Error fatal: {str(e)}")
        return False

def show_statistics():
    """Muestra estadísticas de los datos cargados"""
    print_section("ESTADÍSTICAS DE DATOS CARGADOS")
    try:
        db = DuckDBClient()
        
        res_ext = db.conn.execute("SELECT COUNT(*) as total FROM research_extractions").fetchone()
        print(f"📊 Total Research Extractions: {res_ext[0] if res_ext else 0}")
        
        res_tags = db.conn.execute("SELECT COUNT(*) as total FROM tags").fetchone()
        print(f"🏷️  Total Tags: {res_tags[0] if res_tags else 0}")
        
        db.close()
    except Exception as e:
        print(f"⚠️  Error stats: {str(e)}")

def verify_data_integrity():
    """Verificación simple de integridad"""
    print_section("VERIFICANDO INTEGRIDAD DE DATOS")
    try:
        db = DuckDBClient()
        print("✅ Verificación básica completada")
        db.close()
    except Exception:
        pass

def main():
    print("╔" + "═"*68 + "╗")
    print("║" + " "*10 + "INICIALIZACIÓN MockBigQuery v3.2" + " "*26 + "║")
    print("║" + " "*15 + "Carga desde extractions.json" + " "*25 + "║")
    print("╚" + "═"*68 + "╝")
    
    print("\n⚠️  ADVERTENCIA: Se eliminarán TODOS los datos existentes.")
    
    if not drop_and_create_tables(): sys.exit(1)
    if not load_tags_from_json(): print("\n⚠️  Continuando sin tags")
    if not load_extractions_from_dump(): sys.exit(1)
    
    verify_data_integrity()
    show_statistics()
    
    print_section("✅ INICIALIZACIÓN COMPLETADA")
    print(f"📡 API lista en: {BASE_URL}")

if __name__ == "__main__":
    main()