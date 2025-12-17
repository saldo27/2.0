"""
Generador de claves de licencia para GuardiasApp
Solo para uso interno
"""

import hashlib
import random
import string

def generate_license_key():
    """Generar clave de licencia válida"""
    part1 = "GP"
    part2 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    part3 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    
    # Calcular checksum
    base = f"{part1}-{part2}-{part3}"
    checksum = hashlib. md5(base.encode()).hexdigest()[:4].upper()
    
    return f"{base}-{checksum}"

if __name__ == '__main__':
    print("=" * 50)
    print("  GENERADOR DE CLAVES - GuardiasApp")
    print("=" * 50)
    print("\n🔑 Claves de Licencia Generadas:\n")
    
    for i in range(10):
        key = generate_license_key()
        print(f"{i+1:2d}. {key}")
    
    print("\n" + "=" * 50)
    print("🔐 Clave Maestra:  GUARDIAS-PRO-2025-FULL")
    print("=" * 50)
