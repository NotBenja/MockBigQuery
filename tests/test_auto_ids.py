import requests
import json

base_url = "http://localhost:9000"

def print_section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

# Limpiar
requests.post(f"{base_url}/query", json={"sql": "DROP TABLE IF EXISTS test_ids"})

# Crear tabla
print_section("Crear tabla de prueba")
requests.post(f"{base_url}/create_table", 
    json={
        "name": "test_ids",
        "table_schema": "id INTEGER, producto VARCHAR, precio DECIMAL",
        "primary_key": "id"
    })

# Test 1: Solo IDs auto-generados
print_section("TEST 1: Insertar 3 productos sin IDs")
response = requests.post(f"{base_url}/insert",
    json={
        "table": "test_ids",
        "data": [
            {"producto": "Producto A", "precio": 10.50},
            {"producto": "Producto B", "precio": 20.99},
            {"producto": "Producto C", "precio": 30.00}
        ]
    })
print(json.dumps(response.json(), indent=2))

# Verificar que se crearon IDs 1, 2, 3
response = requests.post(f"{base_url}/query",
    json={"sql": "SELECT * FROM test_ids ORDER BY id"})
data = response.json()["rows"]
print(f"\nIDs generados: {[row['id'] for row in data]}")
assert [row['id'] for row in data] == [1, 2, 3], f"Expected [1,2,3] but got {[row['id'] for row in data]}"

# Test 2: Mix de IDs explícitos y auto-generados
print_section("TEST 2: Mix - ID explícito (10) y sin ID")
response = requests.post(f"{base_url}/insert",
    json={
        "table": "test_ids",
        "data": [
            {"id": 10, "producto": "Producto D", "precio": 40.00},
            {"producto": "Producto E", "precio": 50.00}  # Este debe ser 11 (10+1)
        ]
    })
print(json.dumps(response.json(), indent=2))

# Verificar secuencia: 1,2,3,10,11
response = requests.post(f"{base_url}/query",
    json={"sql": "SELECT * FROM test_ids ORDER BY id"})
data = response.json()["rows"]
ids = [row['id'] for row in data]
print(f"\nIDs después del mix: {ids}")
assert ids == [1, 2, 3, 10, 11], f"Expected [1,2,3,10,11] but got {ids}"

# Test 3: Continuar secuencia después del ID más alto
print_section("TEST 3: Continuar secuencia (debe ser 12)")
response = requests.post(f"{base_url}/insert",
    json={
        "table": "test_ids",
        "data": [
            {"producto": "Producto F", "precio": 60.00}
        ]
    })
print(json.dumps(response.json(), indent=2))

response = requests.post(f"{base_url}/query",
    json={"sql": "SELECT * FROM test_ids ORDER BY id"})
data = response.json()["rows"]
last_id = data[-1]['id']
print(f"\nÚltimo ID generado: {last_id}")
assert last_id == 12, f"Expected 12 but got {last_id}"

# Test 4: Verificar que duplicados en el mismo batch fallan
print_section("TEST 4: Duplicados en el mismo batch (DEBE FALLAR)")
response = requests.post(f"{base_url}/insert",
    json={
        "table": "test_ids",
        "data": [
            {"id": 20, "producto": "Producto G", "precio": 70.00},
            {"id": 20, "producto": "Producto H", "precio": 80.00}  # Duplicado
        ]
    })
print(f"Status Code: {response.status_code}")
print(json.dumps(response.json(), indent=2))
assert response.status_code == 409, f"Expected 409 but got {response.status_code}"

print_section("✓ TODAS LAS PRUEBAS DE IDs PASARON")