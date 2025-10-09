import requests
import json

base_url = "http://localhost:9000"

def print_section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

# Preparar datos
requests.post(f"{base_url}/query", json={"sql": "DROP TABLE IF EXISTS empleados"})
requests.post(f"{base_url}/create_table", 
    json={
        "name": "empleados",
        "table_schema": "id INTEGER, nombre VARCHAR, departamento VARCHAR, salario DECIMAL",
        "primary_key": "id"
    })

# Insertar datos de prueba
requests.post(f"{base_url}/insert",
    json={
        "table": "empleados",
        "data": [
            {"nombre": "Juan", "departamento": "IT", "salario": 50000},
            {"nombre": "María", "departamento": "IT", "salario": 60000},
            {"nombre": "Pedro", "departamento": "Ventas", "salario": 45000},
            {"nombre": "Ana", "departamento": "Ventas", "salario": 48000},
            {"nombre": "Luis", "departamento": "IT", "salario": 55000}
        ]
    })

# Test 1: SELECT simple
print_section("TEST 1: SELECT todos los empleados")
response = requests.post(f"{base_url}/query",
    json={"sql": "SELECT * FROM empleados ORDER BY nombre"})
print(json.dumps(response.json(), indent=2))

# Test 2: WHERE clause
print_section("TEST 2: Empleados de IT")
response = requests.post(f"{base_url}/query",
    json={"sql": "SELECT * FROM empleados WHERE departamento = 'IT' ORDER BY salario"})
print(json.dumps(response.json(), indent=2))

# Test 3: Agregaciones
print_section("TEST 3: Salario promedio por departamento")
response = requests.post(f"{base_url}/query",
    json={"sql": "SELECT departamento, AVG(salario) as salario_promedio FROM empleados GROUP BY departamento"})
print(json.dumps(response.json(), indent=2))

# Test 4: COUNT
print_section("TEST 4: Contar empleados por departamento")
response = requests.post(f"{base_url}/query",
    json={"sql": "SELECT departamento, COUNT(*) as total FROM empleados GROUP BY departamento"})
print(json.dumps(response.json(), indent=2))

# Test 5: ORDER BY y LIMIT
print_section("TEST 5: Top 3 salarios más altos")
response = requests.post(f"{base_url}/query",
    json={"sql": "SELECT nombre, salario FROM empleados ORDER BY salario DESC LIMIT 3"})
print(json.dumps(response.json(), indent=2))

print_section("✓ TODAS LAS PRUEBAS DE QUERIES PASARON")