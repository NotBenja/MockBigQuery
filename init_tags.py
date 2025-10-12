from database import DuckDBClient
import os
import json

def init_tags():
    """Inicializa los tags desde el archivo JSON y vincula tags existentes en data extractions"""
    
    # Ruta al archivo tags.json
    tags_json_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "researchlens_ms-iagen",
        "src",
        "extraction_task",
        "tags",
        "tags.json"
    )
    
    if not os.path.exists(tags_json_path):
        print(f"❌ No se encontró el archivo: {tags_json_path}")
        return
    
    db = DuckDBClient()
    
    try:
        # Verificar si ya hay tags
        existing = db.execute("SELECT COUNT(*) as count FROM tags")
        existing_count = existing[0]['count'] if existing else 0
        
        if existing_count > 0:
            print(f"⚠️  Ya existen {existing_count} tags en la base de datos.")
            response = input("¿Deseas reemplazarlos? (s/n): ")
            if response.lower() != 's':
                print("❌ Operación cancelada")
                return
        
        # Cargar tags
        print(f"📥 Cargando tags desde: {tags_json_path}")
        tags_inserted = db.load_tags_from_json(tags_json_path)
        
        print(f"✅ {tags_inserted} tags insertados exitosamente")
        
        # Mostrar resumen por categoría
        print("\n📊 Resumen por categoría:")
        categories = db.execute("""
            SELECT category, COUNT(*) as count 
            FROM tags 
            GROUP BY category 
            ORDER BY category
        """)
        
        for cat in categories:
            print(f"   {cat['category']}: {cat['count']} tags")
        
        # ============================================================
        # VINCULAR TAGS A DATA EXTRACTIONS EXISTENTES
        # ============================================================
        
        print("\n🔗 Vinculando tags a data extractions existentes...")
        
        # Obtener todos los data extractions
        extractions = db.execute("SELECT id, tags FROM data_extraction_responses")
        
        if not extractions or len(extractions) == 0:
            print("   ℹ️  No hay data extractions para vincular")
        else:
            print(f"   📊 Encontradas {len(extractions)} data extractions")
            
            total_linked = 0
            extractions_processed = 0
            extractions_with_tags = 0
            
            for extraction in extractions:
                extraction_id = extraction['id']
                tags_json = extraction['tags']
                
                # Parsear tags del JSON
                if isinstance(tags_json, str):
                    try:
                        tags = json.loads(tags_json)
                    except:
                        tags = []
                elif isinstance(tags_json, list):
                    tags = tags_json
                else:
                    tags = []
                
                if tags and len(tags) > 0:
                    linked = db.link_tags_to_extraction(extraction_id, tags)
                    total_linked += linked
                    extractions_processed += 1
                    
                    if linked > 0:
                        extractions_with_tags += 1
                    
                    # Mostrar progreso cada 5 extracciones
                    if extractions_processed % 5 == 0:
                        print(f"   ⏳ Procesadas {extractions_processed}/{len(extractions)} extracciones...")
            
            print(f"\n   ✅ Vinculación completada:")
            print(f"      • Extracciones procesadas: {extractions_processed}")
            print(f"      • Extracciones con tags vinculados: {extractions_with_tags}")
            print(f"      • Total de vínculos creados: {total_linked}")
            
            # Verificar resultados
            stats = db.execute("""
                SELECT 
                    COUNT(DISTINCT data_extraction_id) as extractions_linked,
                    COUNT(*) as total_links
                FROM data_extraction_tags
            """)
            
            if stats:
                print(f"\n   📊 Verificación final:")
                print(f"      • Extracciones con vínculos en BD: {stats[0]['extractions_linked']}")
                print(f"      • Total de vínculos en BD: {stats[0]['total_links']}")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise

if __name__ == "__main__":
    init_tags()