# Estrategia de relajación

## Fases del reparto

1. **40 intentos de construcción inicial** → se selecciona el mejor (27-30 huecos vacíos)

2. **Optimizador iterativo:**
   - Greedy fill (3 pasadas)
   - Chain displacement fill (si quedan huecos)

3. **Pasada final en `_try_fill_empty_shifts`:**
   - Pass 1, nivel 0: sin reducción de gap, techo +10%
   - Pass 1, nivel 1: gap −1 si el déficit es ≥ 2; el scoring sigue en +10% de techo exterior
   - Pass 1, nivel 2: mismo scoring que el nivel 1; el techo de asignación pasa a +13%
   - Pass 2 swaps
   - Pass 3A rotate
   - Pass 3B cross-day chain

---

## Niveles de relajación

El sistema tiene tres niveles bien diferenciados:

### Construcción inicial (`schedule_builder.py`)

**Modo estricto (Phase 1 — construcción):**
- Tolerancia ±10% sobre el target
- Gap entre turnos estándar
- Patrón 7/14 y viernes-lunes bloqueados, salvo que el turno sea obligatorio

**Modo relajado (Phase 2 — optimización post-build):**
- Tolerancia sube a ±13% (límite absoluto). Por encima de 13% la asignación se rechaza y se busca otro trabajador
- `relaxation_level=1`: si el trabajador tiene déficit ≥2 turnos, el gap mínimo se reduce en −1 día. El scoring no distingue niveles por encima de 1
- El relleno de huecos usa `_try_fill_empty_shifts` / `_try_fill_empty_shifts_with_worker_order`. Su tercer paso no acorta más el gap: abre el techo de asignación de +10% a +13%

El sistema guarda hasta 40 intentos completos y elige el de mayor cobertura (`_select_best_complete_attempt`). Nunca hay un override de emergencia.

### Optimizador iterativo (`iterative_optimizer.py`)

Solo un mecanismo de relajación: `relaxed_weekend_constraints`, que se activa si lleva ≥3 iteraciones estancado solo con violaciones de fin de semana:
- Pasa de máximo 1 turno de fin de semana por semana a 2
- Añade +1 turno extra a la tolerancia de fin de semana

### Balanceador final (`strict_balance_optimizer.py`)

Escalada de 7 estrategias en orden creciente de agresividad:

| Estrategia                       | `relaxation_level` |
|----------------------------------|--------------------|
| `_try_direct_swap`               | 0 (estricto)       |
| `_try_three_way_swap`            | 0                  |
| `_try_reassignment`              | 0                  |
| `_try_aggressive_three_way_swap` | 1                  |
| `_try_chain_swap`                | 0                  |
| `_try_forced_redistribution`     | 1                  |
| `_try_relaxed_swap`              | 1 (solo si estancado ≥5 iter) |

---

## Lo que NUNCA se relaja (hardcoded)

| Restricción              | Código real                                                      |
|--------------------------|------------------------------------------------------------------|
| `work_periods` / `days_off` | `_is_worker_unavailable()` — sin parámetro de relajación     |
| `incompatible_with`      | Comentario explícito: "Never relax incompatibility constraint"   |
| `mandatory_days`         | `_locked_mandatory` — nunca modificable                         |
| Patrón 7/14              | Prohibido en cualquier asignación que no sea obligatoria. Si el turno que se coloca es `mandatory_days`, puede caer en el intervalo. La validación final quita el turno no obligatorio de un par 7/14. |
| Viernes-lunes            | Prohibido en cualquier asignación que no sea obligatoria, sea cual sea el hueco mínimo. Un turno obligatorio puede caer en viernes y lunes. |
| Tolerancia ±13%          | Límite absoluto del modo relajado. Por encima de 13% no se acepta la asignación y se busca otra. |
| `no_last_post` / `only_last_post` | `_check_hard_constraints()`                             |
| `max_consecutive_weekends` | Comentario: "NEVER RELAX THIS"                                |
