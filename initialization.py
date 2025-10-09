import requests
import json

base_url = "http://localhost:9000"

# Limpiar tabla si existe (para pruebas repetidas)
print("=== Limpiando datos previos ===")
try:
    requests.post(f"{base_url}/query", json={"sql": "DROP TABLE IF EXISTS productos"})
    print("Tabla limpiada")
except:
    print("No había tabla previa")

# Crear tabla con primary key
print("\n=== Creando tabla ===")
response = requests.post(f"{base_url}/create_table", 
    json={
        "name": "productos", 
        "table_schema": "id INTEGER, nombre VARCHAR, precio DECIMAL",
        "primary_key": "id"
    })
print(response.json())

# Insertar datos con IDs explícitos
print("\n=== Insertando datos con IDs ===")
response = requests.post(f"{base_url}/insert",
    json={
        "table": "productos", 
        "data": [
            {"id": 1, "nombre": "Laptop", "precio": 999.99},
            {"id": 2, "nombre": "Mouse", "precio": 29.99}
        ]
    })
print(response.json())

# Insertar datos SIN IDs (se generarán automáticamente)
print("\n=== Insertando datos sin IDs (auto-generados) ===")
response = requests.post(f"{base_url}/insert",
    json={
        "table": "productos", 
        "data": [
            {"nombre": "Teclado", "precio": 79.99},
            {"nombre": "Monitor", "precio": 299.99}
        ]
    })
print(response.json())

# Intentar insertar duplicados (debería fallar)
print("\n=== Intentando insertar duplicados (debe fallar) ===")
response = requests.post(f"{base_url}/insert",
    json={
        "table": "productos", 
        "data": [
            {"id": 1, "nombre": "Laptop Duplicado", "precio": 1099.99}
        ]
    })
print(f"Status Code: {response.status_code}")
print(response.json())

# Intentar crear tabla que ya existe (debería fallar)
print("\n=== Intentando crear tabla existente (debe fallar) ===")
response = requests.post(f"{base_url}/create_table", 
    json={
        "name": "productos", 
        "table_schema": "id INTEGER, nombre VARCHAR"
    })
print(f"Status Code: {response.status_code}")
print(response.json())

# Consultar todos los datos
print("\n=== Consultando todos los datos ===")
response = requests.post(f"{base_url}/query",
    json={"sql": "SELECT * FROM productos ORDER BY id"})
print(json.dumps(response.json(), indent=2))