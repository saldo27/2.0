"""
Generador de claves de licencia para GuardiasApp
Solo para uso interno
"""

import hashlib
import secrets
import string


def generate_license_key():
    """Generar clave de licencia válida (formato GP-XXXX-XXXX-XXXX-YYYYYYYY)"""
    alphabet = string.ascii_uppercase + string.digits
    part1 = "".join(secrets.choice(alphabet) for _ in range(4))
    part2 = "".join(secrets.choice(alphabet) for _ in range(4))
    part3 = "".join(secrets.choice(alphabet) for _ in range(4))

    # Calcular checksum (SHA-256, primeros 8 hex)
    base = f"GP-{part1}-{part2}-{part3}"
    checksum = hashlib.sha256(base.encode()).hexdigest()[:8].upper()

    # Clave completa: GP-XXXX-XXXX-XXXX-YYYYYYYY (26 caracteres)
    return f"{base}-{checksum}"


def verify_key(key):
    """Verificar si una clave GP-XXXX-XXXX-XXXX-YYYYYYYY es válida"""
    # Verificar formato
    if not key.startswith("GP-") or len(key) != 26:
        return False, f"Formato inválido (longitud: {len(key)}, esperado: 26)"

    try:
        parts = key.split("-")
        if len(parts) != 5:
            return False, f"Número de partes incorrecto ({len(parts)}, esperado: 5)"

        # Calcular checksum esperado (SHA-256, primeros 8 hex)
        # parts: ['GP', 'XXXX', 'XXXX', 'XXXX', 'YYYYYYYY']
        base = "-".join(parts[:4])
        expected = hashlib.sha256(base.encode()).hexdigest()[:8].upper()

        if parts[4] == expected:
            return True, "Clave válida ✓"
        else:
            return False, f"Checksum incorrecto (esperado: {expected}, recibido: {parts[4]})"
    except Exception as e:
        return False, f"Error al verificar: {e}"


if __name__ == "__main__":
    print("=" * 60)
    print("  GENERADOR DE CLAVES - GuardiasApp v2.0")
    print("=" * 60)
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

    test_keys = [generate_license_key(), "GP-INVA-LID0-TEST-0000DEAD"]

    for test_key in test_keys:
        is_valid, message = verify_key(test_key)
        status = "✓" if is_valid else "✗"
        print(f"{status} {test_key}")
        print(f"   → {message}")
        print()

    print("=" * 60)
