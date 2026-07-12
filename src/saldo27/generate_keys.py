"""
Generador de claves de licencia para GuardiasApp
Solo para uso interno
"""

import hashlib
import random
import string


def generate_license_key():
    """Generar clave de licencia válida (formato GP-XXXX-XXXX-XXXX-YYYY)"""
    part1 = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    part2 = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    part3 = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))

    # Calcular checksum
    base = f"GP-{part1}-{part2}-{part3}"
    checksum = hashlib.md5(base.encode()).hexdigest()[:4].upper()

    # Clave completa:  GP-XXXX-XXXX-XXXX-YYYY (19 caracteres)
    return f"{base}-{checksum}"


def verify_key(key):
    """Verificar si una clave es válida"""
    # Clave maestra
    if key == "GUARDIAS-PRO-2025-FULL":
        return True, "Clave maestra válida"

    # Verificar formato
    if not key.startswith("GP-") or len(key) != 19:
        return False, f"Formato inválido (longitud: {len(key)}, esperado: 19)"

    try:
        parts = key.split("-")
        if len(parts) != 4:
            return False, f"Número de partes incorrecto ({len(parts)}, esperado: 4)"

        # Calcular checksum esperado
        base = "-".join(parts[:3])
        expected = hashlib.md5(base.encode()).hexdigest()[:4].upper()

        if parts[3] == expected:
            return True, "Clave válida ✓"
        else:
            return False, f"Checksum incorrecto (esperado: {expected}, recibido: {parts[3]})"
    except Exception as e:
        return False, f"Error al verificar:  {e}"


if __name__ == "__main__":
    print("=" * 60)
    print("  GENERADOR DE CLAVES - GuardiasApp v2.0")
    print("=" * 60)
    print()

    # Mostrar clave maestra
    print("🔐 CLAVE MAESTRA:")
    print("   GUARDIAS-PRO-2025-FULL")
    print()

    # Generar y verificar claves
    print("🔑 CLAVES GENERADAS (verificadas):")
    print()

    for i in range(10):
        key = generate_license_key()
        is_valid, message = verify_key(key)
        status = "✓" if is_valid else "✗"
        print(f"{i + 1:2d}. {key}  {status}")

    print()
    print("=" * 60)
    print()

    # Prueba manual
    print("PRUEBA DE VALIDACIÓN:")
    print()

    test_keys = ["GUARDIAS-PRO-2025-FULL", generate_license_key(), "GP-INVALID-KEY-TEST-0000"]

    for test_key in test_keys:
        is_valid, message = verify_key(test_key)
        status = "✓" if is_valid else "✗"
        print(f"{status} {test_key}")
        print(f"   → {message}")
        print()

    print("=" * 60)
