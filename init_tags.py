from database import DuckDBClient
from pathlib import Path
import json
from uuid import uuid4

def init_tags():
    """
    Inicializa solo los tags desde tags.json
    Útil si ya tienes extractions pero quieres recargar tags
    """
    
    tags_file = Path("mock_data") / "tags" / "tags.json"
    
    if not tags_file.exists():
        print(f"❌ No se encontró el archivo: {tags_file}")
        return
    
    db = DuckDBClient()
    
    try:
        # Verificar si ya hay tags
        existing = db.execute("SELECT COUNT(*) as count FROM tags", [])
        existing_count = existing[0]['count'] if existing else 0
        
        if existing_count > 0:
            print(f"⚠️  Ya existen {existing_count} tags en la base de datos.")
            response = input("¿Deseas reemplazarlos? (s/n): ")
            if response.lower() != 's':
                print("❌ Operación cancelada")
                return
            
            # Eliminar tags existentes
            print("🗑️  Eliminando tags existentes...")
            db.conn.execute("DELETE FROM extraction_tags")
            db.conn.execute("DELETE FROM tags")
            print("✓ Tags eliminados")
        
        # Cargar tags desde JSON
        print(f"\n📥 Cargando tags desde: {tags_file}")
        
        with open(tags_file, 'r', encoding='utf-8') as f:
            tags_data = json.load(f)
        
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
                    print(f"   ⚠️  Error al insertar '{tag_name}': {str(e)}")
        
        print(f"\n✅ Total tags insertados: {total_inserted}")
        
        # Mostrar resumen por categoría
        print("\n📊 Resumen por categoría:")
        categories = db.execute("""
            SELECT category, COUNT(*) as count 
            FROM tags 
            GROUP BY category 
            ORDER BY category
        """, [])
        
        for cat in categories:
            print(f"   {cat['category']}: {cat['count']} tags")
        
        # Si hay extractions, vincular tags automáticamente
        result = db.execute("SELECT COUNT(*) as count FROM research_extractions", [])
        extraction_count = result[0]['count'] if result else 0
        
        if extraction_count > 0:
            print(f"\n🔗 Encontradas {extraction_count} research extractions")
            print("   Los tags se vincularán automáticamente al consultar extractions")
        
        db.close()
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise

if __name__ == "__main__":
    print("╔" + "═"*68 + "╗")
    print("║" + " "*20 + "INICIALIZACIÓN DE TAGS v3.0.0" + " "*19 + "║")
    print("╚" + "═"*68 + "╝\n")
    
    init_tags()