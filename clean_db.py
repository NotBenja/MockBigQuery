import duckdb
from pathlib import Path

DB_PATH = "local_bigquery.db"

print("🧹 LIMPIEZA TOTAL DE BASE DE DATOS")
print("="*70)

# Conectar a DB
conn = duckdb.connect(DB_PATH)

# Listar todas las tablas
tables = conn.execute("SHOW TABLES").fetchall()
print(f"\n📋 Tablas encontradas: {len(tables)}")
for table in tables:
    print(f"   • {table[0]}")

# Eliminar TODAS las tablas (incluyendo v2)
print("\n🗑️  Eliminando TODAS las tablas...")
for table in tables:
    try:
        conn.execute(f"DROP TABLE IF EXISTS {table[0]} CASCADE")
        print(f"   ✓ {table[0]} eliminada")
    except Exception as e:
        print(f"   ⚠️  Error en {table[0]}: {str(e)}")

# Verificar
tables_after = conn.execute("SHOW TABLES").fetchall()
print(f"\n✅ Tablas restantes: {len(tables_after)}")

conn.close()

if len(tables_after) == 0:
    print("\n✅ Base de datos completamente limpia")
    print("\nAhora ejecuta:")
    print("   python initialization.py")
else:
    print("\n⚠️  Algunas tablas no se pudieron eliminar")