"""
Sistema de licencias para GuardiasApp
Versión DEMO con limitación por número de usos
"""

import json
import os
from datetime import datetime
from pathlib import Path
import hashlib

class LicenseManager:
    """Gestor de licencias y limitaciones DEMO"""
    
    def __init__(self):
        # Carpeta de configuración en AppData del usuario
        self.config_dir = Path.home() / ".guardiasapp"
        self.config_dir. mkdir(exist_ok=True)
        self.license_file = self.config_dir / "license.dat"
        self.usage_file = self.config_dir / "usage.dat"
        
        # Limitaciones DEMO
        self. DEMO_MAX_USES = 10  # Máximo 10 generaciones de horarios
        self.DEMO_MAX_WORKERS = 15  # Máximo 15 trabajadores
        self.DEMO_MAX_DAYS = 62  # Máximo 31 días de horario
        self.DEMO_WATERMARK = True  # Marca de agua en PDFs
        
    def is_licensed(self):
        """Verificar si hay licencia válida"""
        if not self. license_file.exists():
            return False
        
        try: 
            with open(self.license_file, 'r') as f:
                license_data = json.load(f)
            
            # Verificar licencia
            license_key = license_data.get('key', '')
            if self._validate_license_key(license_key):
                return True
        except:
            return False
        
        return False
    
    def _validate_license_key(self, key):
        """Validar clave de licencia"""
        # Clave maestra
        MASTER_KEY = "GUARDIAS-PRO-2025-FULL"
        if key == MASTER_KEY:
            return True
        
        # Verificar formato GP-XXXX-XXXX-XXXX
        if key.startswith("GP-") and len(key) == 19:
            return self._verify_checksum(key)
        
        return False
    
    def _verify_checksum(self, key):
        """Verificar checksum de la clave"""
        try:
            parts = key. split('-')
            if len(parts) != 4:
                return False
            
            # Calcular hash
            base = '-'.join(parts[: 3])
            expected = hashlib.md5(base.encode()).hexdigest()[:4].upper()
            return parts[3] == expected
        except:
            return False
    
    def get_usage_stats(self):
        """Obtener estadísticas de uso"""
        if not self.usage_file. exists():
            return {
                'uses': 0,
                'first_use': None,
                'last_use': None
            }
        
        try: 
            with open(self.usage_file, 'r') as f:
                return json.load(f)
        except:
            return {'uses':  0, 'first_use': None, 'last_use': None}
    
    def increment_usage(self):
        """Incrementar contador de uso"""
        stats = self.get_usage_stats()
        
        now = datetime.now().isoformat()
        
        if stats['uses'] == 0:
            stats['first_use'] = now
        
        stats['uses'] += 1
        stats['last_use'] = now
        
        with open(self.usage_file, 'w') as f:
            json.dump(stats, f)
        
        return stats['uses']
    
    def can_use(self):
        """Verificar si puede usar la aplicación"""
        # Si tiene licencia, puede usar sin límites
        if self.is_licensed():
            return True, "Licencia válida ✅", None
        
        # Modo DEMO - verificar límites
        stats = self.get_usage_stats()
        uses = stats['uses']
        
        if uses >= self.DEMO_MAX_USES:
            return False, "Límite de usos alcanzado", uses
        
        remaining = self.DEMO_MAX_USES - uses
        return True, f"Modo DEMO - {remaining} usos restantes", remaining
    
    def get_limitations(self):
        """Obtener limitaciones actuales"""
        if self.is_licensed():
            return {
                'max_workers': None,  # Sin límite
                'max_days': None,  # Sin límite
                'watermark': False,
                'mode': 'FULL'
            }
        
        return {
            'max_workers':  self.DEMO_MAX_WORKERS,
            'max_days':  self.DEMO_MAX_DAYS,
            'watermark':  self.DEMO_WATERMARK,
            'mode': 'DEMO'
        }
    
    def activate_license(self, license_key):
        """Activar licencia"""
        license_key = license_key.strip().upper()
        
        if self._validate_license_key(license_key):
            license_data = {
                'key': license_key,
                'activated': datetime.now().isoformat()
            }
            
            with open(self.license_file, 'w') as f:
                json.dump(license_data, f)
            
            return True, "✅ Licencia activada correctamente"
        
        return False, "❌ Clave de licencia inválida"
    
    def reset_demo(self):
        """Resetear demo (solo para testing)"""
        if self.usage_file.exists():
            os.remove(self.usage_file)
        if self.license_file.exists():
            os.remove(self. license_file)


# Instancia global
license_manager = LicenseManager()
