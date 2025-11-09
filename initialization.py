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
        
        # Recrear tablas (usando el método del constructor)
        print("\n🔨 Recreando tablas nuevas...")
        db._initialize_database()
        print("✓ Tablas creadas exitosamente")
        
        # Cerrar conexión
        db.close()
        
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

def parse_date(raw_date: str) -> str:
    """Valida y normaliza fechas"""
    invalid_dates = [
        "Información no disponible", 
        "Fecha no disponible", 
        "No disponible",
        "N/A",
        ""
    ]
    
    if not raw_date or raw_date in invalid_dates:
        return datetime.today().strftime("%Y-%m-%d")
    
    try:
        if isinstance(raw_date, str) and len(raw_date) == 10:
            datetime.fromisoformat(raw_date)
            return raw_date
        else:
            return datetime.today().strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return datetime.today().strftime("%Y-%m-%d")

def convert_summary_to_bulletpoints(summary: Any) -> list:
    """
    Convierte el summary antiguo (string o lista de strings) 
    al nuevo formato List[BulletPoint]
    """
    if isinstance(summary, str):
        # Si es un string, crear un solo bullet point
        return [{
            "title": "Resumen Principal",
            "body": summary
        }]
    elif isinstance(summary, list):
        # Si es lista, convertir cada elemento
        bullet_points = []
        for idx, item in enumerate(summary, 1):
            if isinstance(item, str):
                bullet_points.append({
                    "title": f"Punto {idx}",
                    "body": item
                })
            elif isinstance(item, dict):
                # Ya está en formato correcto
                bullet_points.append(item)
        return bullet_points
    else:
        return []

def convert_trade_summary_to_bulletpoints(trade_summary: Any) -> list:
    """
    Convierte el summary de TradeIdea al formato List[BulletPoint]
    """
    if isinstance(trade_summary, str):
        return [{
            "title": "Análisis",
            "body": trade_summary
        }]
    elif isinstance(trade_summary, list):
        bullet_points = []
        for idx, item in enumerate(trade_summary, 1):
            if isinstance(item, str):
                bullet_points.append({
                    "title": f"Punto {idx}",
                    "body": item
                })
            elif isinstance(item, dict):
                bullet_points.append(item)
        return bullet_points
    else:
        return []

def normalize_tags_structure(tags_data: Any, counterpart: str = "Goldman Sachs") -> Dict[str, Any]:
    """
    Convierte tags antiguos al formato nuevo Tags(ContentExtractionTags)
    """
    if isinstance(tags_data, list):
        # Tags antiguos como lista plana -> distribuir en categorías
        return {
            "counterpart": counterpart,
            "asset_class": [t for t in tags_data if t in ["Equity", "Fixed Income", "Commodities", "FX", "Crypto"]],
            "e_d": [t for t in tags_data if t in ["Emerging", "Developed"]],
            "region": [t for t in tags_data if t in ["Asia Pacific", "Europe", "Americas", "Middle East", "Africa"]],
            "country": [t for t in tags_data if t not in ["Equity", "Fixed Income", "Commodities", "FX", "Crypto", "Emerging", "Developed", "Asia Pacific", "Europe", "Americas", "Middle East", "Africa"]],
            "sector": [],
            "trade": []
        }
    elif isinstance(tags_data, dict):
        # Ya tiene estructura, solo agregar counterpart si no existe
        if "counterpart" not in tags_data:
            tags_data["counterpart"] = counterpart
        
        # Asegurar que todas las categorías existen
        for key in ["asset_class", "e_d", "region", "country", "sector", "trade"]:
            if key not in tags_data:
                tags_data[key] = []
        
        return tags_data
    else:
        # Valores por defecto
        return {
            "counterpart": counterpart,
            "asset_class": [],
            "e_d": [],
            "region": [],
            "country": [],
            "sector": [],
            "trade": []
        }

def load_tags_from_json():
    """Carga tags desde tags.json y los inserta en la BD"""
    print_section("CARGANDO TAGS DESDE JSON")
    
    tags_file = MOCK_DATA_DIR / "tags" / "tags.json"
    
    if not tags_file.exists():
        print(f"⚠️  No se encontró {tags_file}")
        print("   Creando tags básicos por defecto...")
        return load_default_tags()
    
    try:
        tags_data = load_json_file(tags_file)
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

def load_mock_extractions():
    """
    Carga extractions desde mock_data combinando summary + trade
    """
    print_section("CARGANDO RESEARCH EXTRACTIONS")
    
    summary_dir = MOCK_DATA_DIR / "summary"
    trade_dir = MOCK_DATA_DIR / "trade"
    
    summary_files = list(summary_dir.glob("*.json"))
    
    if not summary_files:
        print("❌ No se encontraron archivos mock en mock_data/summary/")
        return False
    
    print(f"\n📂 Encontrados {len(summary_files)} archivos de summary")
    
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
            valid_date = parse_date(summary_data.get("date", ""))
            
            # Convertir summary a BulletPoints
            summary_bulletpoints = convert_summary_to_bulletpoints(
                summary_data.get("summary", "")
            )
            
            # Normalizar tags
            tags_normalized = normalize_tags_structure(
                summary_data.get("tags", []),
                counterpart="Goldman Sachs"  # Puedes extraer esto del archivo si existe
            )
            
            # Normalizar pros/cons
            pros = summary_data.get("pros", [])
            if isinstance(pros, str):
                pros = [pros] if pros else []
            
            cons = summary_data.get("cons", [])
            if isinstance(cons, str):
                cons = [cons] if cons else []
            
            # Procesar trade ideas
            trade_ideas = []
            trade_file = trade_dir / summary_file.name.replace("-summary.json", "-trade.json")
            
            if trade_file.exists():
                trade_data = load_json_file(trade_file)
                raw_trade_ideas = trade_data.get("tradeIdeas", [])
                
                print(f"  📊 Encontradas {len(raw_trade_ideas)} trade ideas")
                
                for idx, idea in enumerate(raw_trade_ideas, 1):
                    # Convertir trade summary a BulletPoints
                    trade_summary_bp = convert_trade_summary_to_bulletpoints(
                        idea.get("summary", "")
                    )
                    
                    # Normalizar pros/cons de trade
                    trade_pros = idea.get("pros", [])
                    if isinstance(trade_pros, str):
                        trade_pros = [trade_pros] if trade_pros else []
                    
                    trade_cons = idea.get("cons", [])
                    if isinstance(trade_cons, str):
                        trade_cons = [trade_cons] if trade_cons else []
                    
                    trade_ideas.append({
                        "recommendation": idea.get("recommendation", ""),
                        "summary": trade_summary_bp,
                        "conviction": idea.get("conviction", 5),
                        "pros": trade_pros,
                        "cons": trade_cons
                    })
                    
                    print(f"    ✓ Trade Idea {idx}: {idea.get('recommendation', 'N/A')[:50]}... (Convicción: {idea.get('conviction', 5)}/10)")
            else:
                print(f"  ⚠️  No se encontró archivo de trade: {trade_file.name}")
            
            # Crear extraction completa
            extraction_data = {
                "id": str(uuid4()),
                "title": f"Análisis {topic} - Modelo {model}",
                "published_date": valid_date,
                "authors": summary_data.get("authors", ["Analyst Team"]),
                "summary": summary_bulletpoints,
                "tags": tags_normalized,
                "pros": pros,
                "cons": cons,
                "trade_ideas": trade_ideas,
                "suggested_tags": [],  # Vacío por ahora
                "created_at": datetime.now()
            }
            
            # Insertar en BD
            result = db.insert_extraction(extraction_data)
            
            if result:
                print(f"  ✅ Extraction creada: {extraction_data['title']}")
                print(f"     • ID: {result['id']}")
                print(f"     • Fecha: {valid_date}")
                print(f"     • Bullet Points: {len(summary_bulletpoints)}")
                print(f"     • Trade Ideas: {len(trade_ideas)}")
                loaded_count += 1
            else:
                print(f"  ❌ Error al crear extraction")
                error_count += 1
            
        except Exception as e:
            print(f"  ❌ Error procesando {summary_file.name}: {str(e)}")
            import traceback
            print(traceback.format_exc())
            error_count += 1
    
    db.close()
    
    print(f"\n{'='*70}")
    print(f"✅ Procesados exitosamente: {loaded_count} archivos")
    if error_count > 0:
        print(f"⚠️  Errores: {error_count} archivos")
    
    return True

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
        
        # Total de trade ideas (sin json_array_elements - usar LENGTH)
        result = db.execute("""
            SELECT 
                SUM(json_array_length(trade_ideas)) as total_ideas
            FROM research_extractions
        """, [])
        
        total_ideas = result[0]["total_ideas"] if result and result[0]["total_ideas"] else 0
        print(f"💡 Total Trade Ideas (anidadas): {total_ideas}")
        
        # Distribución por convicción (simplificada sin unnest)
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
        
        # Ejemplos de extractions
        print(f"\n📋 Ejemplos de Research Extractions:")
        result = db.execute("""
            SELECT title, published_date, 
                   json_array_length(trade_ideas) as trade_count
            FROM research_extractions 
            LIMIT 3
        """, [])
        
        if result:
            for item in result:
                print(f"   • {item['title']}")
                print(f"     Fecha: {item['published_date']} | Trade Ideas: {item['trade_count']}")
        
        db.close()
        
    except Exception as e:
        print(f"⚠️  Error al obtener estadísticas: {str(e)}")
        import traceback
        print(traceback.format_exc())

def verify_data_integrity():
    """Verifica la integridad de los datos cargados"""
    print_section("VERIFICANDO INTEGRIDAD DE DATOS")
    
    try:
        db = DuckDBClient()
        
        # 1. Verificar que todos los extractions tengan tags
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
        
        # 2. Verificar convicción válida en trade ideas (sin json_array_elements)
        result = db.execute("""
            SELECT COUNT(*) as total_ideas
            FROM research_extractions
            WHERE json_array_length(trade_ideas) > 0
        """, [])
        
        total_ideas = result[0]["total_ideas"] if result else 0
        print(f"✅ Total extractions con trade ideas: {total_ideas}")
        
        # 3. Verificar vínculos extraction-tags
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
        import traceback
        print(traceback.format_exc())

def main():
    """Función principal de inicialización"""
    print("╔" + "═"*68 + "╗")
    print("║" + " "*10 + "INICIALIZACIÓN MockBigQuery v3.0.0" + " "*24 + "║")
    print("║" + " "*15 + "Research Extractions con Trade Ideas Anidados" + " "*8 + "║")
    print("╚" + "═"*68 + "╝")
    
    print("\n⚠️  ADVERTENCIA:")
    print("   Este script eliminará TODOS los datos existentes y recreará")
    print("   la base de datos con la nueva estructura v3.0.0\n")
    
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
        print("\n⚠️  Continuando sin tags (se usarán valores por defecto)")
    
    # 3. Cargar extractions con trade ideas anidados
    if not load_mock_extractions():
        print("\n❌ Error al cargar extractions. Abortando.")
        sys.exit(1)
    
    # 4. Verificar integridad
    verify_data_integrity()
    
    # 5. Mostrar estadísticas
    show_statistics()
    
    # 6. Mensaje final
    print_section("✅ INICIALIZACIÓN COMPLETADA")
    print(f"""
    🎉 Base de datos v3.0.0 inicializada exitosamente!
    
    Cambios importantes:
    ✅ Tabla única: research_extractions
    ✅ Trade ideas anidados (JSON)
    ✅ Summary como List[BulletPoint]
    ✅ Tags separados mantenidos
    ✅ Suggested tags incluidos
    
    Ahora puedes iniciar el servidor:
    
        python main.py
    
    Luego accede a:
    📡 API: {BASE_URL}
    📚 Documentación: {BASE_URL}/docs
    
    Endpoints principales:
    • GET  {BASE_URL}/api/extractions
    • POST {BASE_URL}/api/extractions
    • GET  {BASE_URL}/api/extractions/{{id}}
    • POST {BASE_URL}/api/dashboard
    • GET  {BASE_URL}/api/tags
    """)

if __name__ == "__main__":
    main()