# ADR 0001 — Arquitectura por capas para scheduling

## Estado
Aceptado

## Contexto
El sistema mezclaba UI, casos de uso, orquestación y carga opcional de motores en módulos centrales, lo que dificultaba pruebas unitarias y evolución del pipeline.

## Decisión
1. Separar capas en `application`, `domain`, `infrastructure`.
2. Introducir `ScheduleState` inmutable (`domain`) como snapshot tipado entre fases.
3. Estandarizar pipeline declarativo en `application.pipeline` con fases explícitas: `initialize`, `mandatory`, `distribution`, `finalize`.
4. Mantener `SchedulerCore` como orquestador ligero que ejecuta fases y registra trazabilidad por fase.
5. Centralizar carga de motores opcionales en `infrastructure.optional_engines`.
6. Definir contratos estables de motores en `application.contracts` (`build/optimize/validate/finalize`).

## Consecuencias
- Menor acoplamiento entre UI y núcleo.
- Menor dispersión de lógica opcional (`ImportError`) en el core.
- Mejor testabilidad de pipeline, estado y plugins.
- Migración progresiva: la lógica interna de cada fase permanece intacta y puede desacoplarse por iteraciones futuras.
