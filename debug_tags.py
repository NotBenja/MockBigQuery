"""
Script de diagnóstico para verificar vínculos de tags
"""
from database import DuckDBClient
import json

def debug_tags():
    """Diagnostica el problema de tags"""
    print("="*70)
    print("🔍 DIAGNÓSTICO DE TAGS")
    print("="*70)
    
    db = DuckDBClient()
    
    # 1. Verificar tags en la tabla tags
    print("\n1️⃣ TAGS EN TABLA 'tags':")
    tags = db.execute("SELECT category, name FROM tags ORDER BY category, name", [])
    
    tags_by_category = {}
    for tag in tags:
        cat = tag['category']
        if cat not in tags_by_category:
            tags_by_category[cat] = []
        tags_by_category[cat].append(tag['name'])
    
    for cat, names in tags_by_category.items():
        print(f"\n   📂 {cat}: ({len(names)} tags)")
        for name in names[:5]:  # Mostrar solo los primeros 5
            print(f"      • {name}")
        if len(names) > 5:
            print(f"      ... y {len(names) - 5} más")
    
    # 2. Verificar extractions y sus tags JSON
    print("\n2️⃣ TAGS EN research_extractions (campo JSON):")
    extractions = db.execute("""
        SELECT id, title, tags 
        FROM research_extractions
    """, [])
    
    for ext in extractions:
        print(f"\n   📄 {ext['title']}")
        print(f"      ID: {ext['id']}")
        tags_obj = ext['tags']
        
        print(f"      • counterpart: {tags_obj.get('counterpart')}")
        print(f"      • asset_class: {tags_obj.get('asset_class', [])}")
        print(f"      • e_d: {tags_obj.get('e_d', [])}")
        print(f"      • region: {tags_obj.get('region', [])}")
        print(f"      • country: {tags_obj.get('country', [])}")
        print(f"      • sector: {tags_obj.get('sector', [])}")
        print(f"      • trade: {tags_obj.get('trade', [])}")
    
    # 3. Verificar vínculos en extraction_tags
    print("\n3️⃣ VÍNCULOS EN extraction_tags:")
    links = db.execute("""
        SELECT COUNT(*) as total FROM extraction_tags
    """, [])
    
    total_links = links[0]['total'] if links else 0
    print(f"   Total vínculos: {total_links}")
    
    if total_links > 0:
        links_detail = db.execute("""
            SELECT 
                re.title,
                t.name as tag_name,
                t.category as tag_category
            FROM extraction_tags et
            JOIN research_extractions re ON et.extraction_id = re.id
            JOIN tags t ON et.tag_id = t.id
            ORDER BY re.title, t.category, t.name
        """, [])
        
        current_title = None
        for link in links_detail:
            if link['title'] != current_title:
                current_title = link['title']
                print(f"\n   📄 {current_title}")
            print(f"      • {link['tag_category']}: {link['tag_name']}")
    else:
        print("   ⚠️  NO HAY VÍNCULOS - Este es el problema!")
    
    # 4. Verificar coincidencias entre tags JSON y tabla tags
    print("\n4️⃣ VERIFICACIÓN DE COINCIDENCIAS:")
    
    for ext in extractions:
        print(f"\n   📄 {ext['title']}")
        tags_obj = ext['tags']
        
        # Verificar cada categoría
        for category_key in ['asset_class', 'e_d', 'region', 'country', 'sector', 'trade']:
            tag_names = tags_obj.get(category_key, [])
            
            if tag_names:
                print(f"\n      📂 Categoría '{category_key}':")
                for tag_name in tag_names:
                    # Buscar en tabla tags
                    result = db.execute("""
                        SELECT id FROM tags 
                        WHERE name = ? AND category = ?
                    """, [tag_name, category_key])
                    
                    if result:
                        print(f"         ✅ '{tag_name}' encontrado en tabla tags")
                    else:
                        print(f"         ❌ '{tag_name}' NO encontrado en tabla tags")
                        
                        # Buscar similares
                        similar = db.execute("""
                            SELECT name, category FROM tags 
                            WHERE category = ? AND name LIKE ?
                            LIMIT 3
                        """, [category_key, f"%{tag_name[:3]}%"])
                        
                        if similar:
                            print(f"            Similares encontrados:")
                            for s in similar:
                                print(f"              • {s['name']} ({s['category']})")
        
        # Verificar counterpart
        counterpart = tags_obj.get('counterpart')
        if counterpart:
            result = db.execute("""
                SELECT id FROM tags 
                WHERE name = ? AND category = 'counterpart'
            """, [counterpart])
            
            if result:
                print(f"\n      ✅ Counterpart '{counterpart}' encontrado")
            else:
                print(f"\n      ❌ Counterpart '{counterpart}' NO encontrado")
    
    # 5. Verificar categorías en tags.json vs código
    print("\n5️⃣ CATEGORÍAS EN TABLA TAGS:")
    categories = db.execute("""
        SELECT DISTINCT category FROM tags ORDER BY category
    """, [])
    
    print("   Categorías encontradas:")
    for cat in categories:
        print(f"      • {cat['category']}")
    
    print("\n   Categorías esperadas por el código:")
    expected = ['asset_class', 'e_d', 'region', 'country', 'sector', 'trade', 'counterpart']
    for exp in expected:
        print(f"      • {exp}")
    
    db.close()
    
    print("\n" + "="*70)
    print("✅ DIAGNÓSTICO COMPLETADO")
    print("="*70)

if __name__ == "__main__":
    debug_tags()