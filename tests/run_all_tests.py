import subprocess
import sys

tests = [
    "test_basic.py",
    "test_auto_ids.py",
    "test_duplicates.py",
    "test_queries.py"
]

print("="*70)
print("  EJECUTANDO SUITE COMPLETA DE PRUEBAS")
print("="*70)

failed = []
for test in tests:
    print(f"\n\n{'='*70}")
    print(f"  EJECUTANDO: {test}")
    print(f"{'='*70}\n")
    
    result = subprocess.run([sys.executable, test], capture_output=False)
    
    if result.returncode != 0:
        failed.append(test)

print("\n\n" + "="*70)
if failed:
    print(f"  ❌ FALLARON {len(failed)} PRUEBAS:")
    for test in failed:
        print(f"     - {test}")
else:
    print("  ✅ TODAS LAS PRUEBAS PASARON EXITOSAMENTE")
print("="*70)