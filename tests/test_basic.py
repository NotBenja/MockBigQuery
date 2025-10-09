import requests
import json

base_url = "http://localhost:9000"

def print_section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def print_response(response):
    print(f"Status Code: {response.status_code}")
    print(json.dumps(response.json(), indent=2))

# Limpiar
print_section("LIMPIANDO DATOS PREVIOS")
requests.post(f"{base_url}/query", json={"sql": "DROP TABLE IF EXISTS test_usuarios"})
print("✓ Limpieza completada")

# Test 1: Crear tabla
print_section("TEST 1: Crear tabla con PRIMARY KEY")
response = requests.post(f"{base_url}/create_table", 
    json={
        "name": "test_usuarios",
        "table_schema": "id INTEGER, nombre VARCHAR, email VARCHAR, edad INTEGER",
        "primary_key": "id"
    })
print_response(response)
assert response.status_code == 200

# Test 2: Insertar con IDs explícitos
print_section("TEST 2: Insertar filas con IDs explícitos")
response = requests.post(f"{base_url}/insert",
    json={
        "table": "test_usuarios",
        "data": [
            {"id": 1, "nombre": "Juan", "email": "juan@test.com", "edad": 25},
            {"id": 2, "nombre": "María", "email": "maria@test.com", "edad": 30}
        ]
    })
print_response(response)
assert response.status_code == 200
assert response.json()["rows"] == 2

# Test 3: Insertar sin IDs (auto-generados)
print_section("TEST 3: Insertar filas SIN IDs (auto-generados)")
response = requests.post(f"{base_url}/insert",
    json={
        "table": "test_usuarios",
        "data": [
            {"nombre": "Pedro", "email": "pedro@test.com", "edad": 35},
            {"nombre": "Ana", "email": "ana@test.com", "edad": 28}
        ]
    })
print_response(response)
assert response.status_code == 200
assert response.json()["rows"] == 2

# Test 4: Consultar todos los datos
print_section("TEST 4: Consultar todos los usuarios")
response = requests.post(f"{base_url}/query",
    json={"sql": "SELECT * FROM test_usuarios ORDER BY id"})
print_response(response)
assert len(response.json()["rows"]) == 4

# Test 5: Intentar insertar ID duplicado (debe fallar)
print_section("TEST 5: Intentar insertar ID duplicado (DEBE FALLAR)")
response = requests.post(f"{base_url}/insert",
    json={
        "table": "test_usuarios",
        "data": [
            {"id": 1, "nombre": "Duplicado", "email": "dup@test.com", "edad": 40}
        ]
    })
print_response(response)
assert response.status_code == 409

# Test 6: Intentar crear tabla existente (debe fallar)
print_section("TEST 6: Intentar crear tabla existente (DEBE FALLAR)")
response = requests.post(f"{base_url}/create_table",
    json={
        "name": "test_usuarios",
        "table_schema": "id INTEGER, nombre VARCHAR"
    })
print_response(response)
assert response.status_code == 409

print_section("✓ TODAS LAS PRUEBAS PASARON EXITOSAMENTE")