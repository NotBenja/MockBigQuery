import requests
import json

base_url = "http://localhost:9000"

# Crear tabla
response = requests.post(f"{base_url}/create_table", 
    json={"name": "productos", "table_schema": "id INTEGER, nombre VARCHAR, precio DECIMAL"})
print(response.json())

# Insertar datos
response = requests.post(f"{base_url}/insert",
    json={
        "table": "productos", 
        "data": [
            {"id": 1, "nombre": "Laptop", "precio": 999.99},
            {"id": 2, "nombre": "Mouse", "precio": 29.99}
        ]
    })
print(response.json())

# Consultar datos
response = requests.post(f"{base_url}/query",
    json={"sql": "SELECT * FROM productos WHERE precio > 50"})
print(response.json())