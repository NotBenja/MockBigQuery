import requests
import json

base_url = "http://localhost:9000"

def print_section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

# Limpiar
requests.post(f"{base_url}/query", json={"sql": "DROP TABLE IF EXISTS test_dup"})

# Crear tabla
requests.post(f"{base_url}/create_table", 
    json={
        "name": "test_dup",
        "table_schema": "id INTEGER, item VARCHAR",
        "primary_key": "id"
    })

# Insertar datos iniciales
print_section("Insertar datos iniciales")
response = requests.post(f"{base_url}/insert",
    json={
        "table": "test_dup",
        "data": [
            {"id": 1, "item": "Item 1"},
            {"id": 2, "item": "Item 2"},
            {"id": 3, "item": "Item 3"}
        ]
    })
print(json.dumps(response.json(), indent=2))

# Test 1: Duplicado simple
print_section("TEST 1: Intentar duplicar ID 1")
response = requests.post(f"{base_url}/insert",
    json={
        "table": "test_dup",
        "data": [{"id": 1, "item": "Item Duplicado"}]
    })
print(f"Status: {response.status_code}")
print(json.dumps(response.json(), indent=2))
assert response.status_code == 409
assert "duplicados" in response.json()["detail"].lower()

# Test 2: Múltiples duplicados
print_section("TEST 2: Intentar duplicar IDs 1 y 3")
response = requests.post(f"{base_url}/insert",
    json={
        "table": "test_dup",
        "data": [
            {"id": 1, "item": "Dup 1"},
            {"id": 3, "item": "Dup 3"},
            {"id": 4, "item": "Nuevo"}
        ]
    })
print(f"Status: {response.status_code}")
print(json.dumps(response.json(), indent=2))
assert response.status_code == 409

# Test 3: Mix duplicado en el mismo batch
print_section("TEST 3: Duplicado dentro del mismo batch")
response = requests.post(f"{base_url}/insert",
    json={
        "table": "test_dup",
        "data": [
            {"id": 10, "item": "Nuevo 10"},
            {"id": 10, "item": "Duplicado en batch"}  # Mismo ID
        ]
    })
print(f"Status: {response.status_code}")
print(json.dumps(response.json(), indent=2))

# Verificar que no se insertó nada
response = requests.post(f"{base_url}/query",
    json={"sql": "SELECT COUNT(*) as total FROM test_dup"})
total = response.json()["rows"][0]["total"]
print(f"\nTotal de registros (debe ser 3): {total}")
assert total == 3

print_section("✓ TODAS LAS PRUEBAS DE DUPLICADOS PASARON")