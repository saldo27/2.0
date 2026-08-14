# Saldo27 — Agent Directives

Shared guidelines for AI coding assistants (Claude, Copilot, etc.) working on this project.

**Current version: 3.2 (Agosto 2026)**

## Project overview

Saldo27 is a **worker shift scheduling system** for medical staff, built with Streamlit. It generates optimised monthly schedules respecting constraints like minimum gaps between shifts, maximum consecutive weekends, incompatibilities between workers, and mandatory/off days.

## Architecture

```
src/saldo27/              # All source code — installable Python package
  application/            # Use-cases, pipeline, generation flow, contracts
  domain/                 # Domain models: schedule state, engine state mixin
  infrastructure/         # Optional engines, external integrations
tests/                    # pytest test suite (unit + e2e)
  e2e/                    # Playwright browser tests against the Streamlit app
docs/                     # Project documentation
packaging/                # PyInstaller hooks, .spec file, Windows installer
```

### Key modules

| Module | Role |
|--------|------|
| `app_streamlit.py` | Streamlit UI — the main entry point (v3.2) |
| `scheduler.py` | Top-level Scheduler orchestrator |
| `scheduler_core.py` | Core optimisation loop |
| `schedule_builder.py` | Initial schedule construction |
| `iterative_optimizer.py` | Post-build iterative improvement |
| `constraint_checker.py` | Validates all scheduling constraints |
| `balance_validator.py` | Checks workload distribution fairness |
| `data_manager.py` | Worker data and schedule state management |
| `utilities.py` | Date/time helpers, holiday detection |
| `event_bus.py` | Internal pub/sub event system |
| `performance_cache.py` | Caching decorators and monitoring |
| `bridge_manager.py` | Detection and balancing of bridge-holiday shifts |
| `change_tracker.py` | Undo/redo and audit trail for real-time edits |
| `real_time_engine.py` | Unified real-time processing for schedule operations |
| `live_validator.py` | Instant constraint validation during manual edits |
| `license_manager.py` | Demo/full-license management (usage limits, key activation) |
| `prior_schedule_handler.py` | Parses exported JSON to extract cross-period constraints |
| `schedule_analyzer.py` | PDF/Excel/CSV schedule reader; generates analysis reports |
| `final_adjustment_engine.py` | Post-generation balancing of remaining deviations |
| `adaptive_iterations.py` | Dynamically adjusts optimiser iteration counts |
| `adjustment_utils.py` | Shared helpers for shift adjustment operations |
| `demand_forecaster.py` | Demand-forecasting model for predictive analytics |
| `predictive_analytics.py` | Insights, recommendations, and demand trend analysis |
| `predictive_optimizer.py` | Applies predictive recommendations to the scheduler |
| `historical_data_manager.py` | Persists and queries historical scheduling data |
| `statistics_calculator.py` | Aggregates per-worker and global statistics |
| `optimization_metrics.py` | Tracks and exposes quality metrics from each run |
| `operation_prioritizer.py` | Ranks candidate assignment operations by priority |
| `incremental_updater.py` | Applies incremental schedule mutations efficiently |
| `progress_monitor.py` | Reports optimiser progress to the UI |
| `scheduler_initializer.py` | Bootstraps scheduler state before the core loop |
| `scheduler_reporting.py` | Formats and exports scheduler results |
| `scheduler_tracking.py` | Records per-run tracking data |
| `scheduler_validation.py` | Pre-run validation of configuration and worker data |
| `scheduler_config.py` | `SchedulerConfig` dataclass and logging setup |
| `worker_eligibility.py` | Determines eligible workers for each shift slot |
| `shift_tolerance_validator.py` | Validates per-worker shift-count tolerances |
| `target_calculator.py` | Computes per-worker target shift counts |
| `exceptions.py` | Custom exception hierarchy |
| `pdf_exporter.py` | PDF calendar and summary generation |
| `generate_keys.py` | License-key generation utility |
| `run_app.py` | `saldo27` CLI entry-point (runs Streamlit) |

### Application layer (`src/saldo27/application/`)

| Module | Role |
|--------|------|
| `use_cases.py` | `GenerateScheduleRequest` / `run_simulation` — application use-cases |
| `generation_flow.py` | `execute_generation_workflow`, `prepare_generation_workflow`, UI callback types |
| `pipeline.py` | Pipeline orchestration across use-cases |
| `contracts.py` | Shared data-transfer objects and contracts between layers |

### Domain layer (`src/saldo27/domain/`)

| Module | Role |
|--------|------|
| `schedule_state.py` | `ScheduleState` — authoritative in-memory schedule model |
| `engine_state_mixin.py` | Mixin providing engine-phase state tracking |

### Infrastructure layer (`src/saldo27/infrastructure/`)

| Module | Role |
|--------|------|
| `optional_engines.py` | Conditionally loads OR-Tools and other optional engines |

### Balance/distribution engines (`scheduler_core.py`'s `_iterative_improvement_phase`)

Three modules apply shift-balance adjustments during Phase 3 of optimization. They are **not**
alternatives to pick from — they run in sequence, each with a distinct scope:

| Module | Role | Instantiated by |
|--------|------|------------------|
| `advanced_distribution_engine.py` (`AdvancedDistributionEngine`) | Runs first; broad post/weekday redistribution passes | `scheduler_core.py` (`_iterative_improvement_phase`) |
| `strict_balance_optimizer.py` (`StrictBalanceOptimizer`) | Runs after the advanced engine; enforces exact `target_shifts` balance per worker | `scheduler_core.py` (`_iterative_improvement_phase`) |
| `balance_validator.py` (`BalanceValidator`) | Used separately by `IterativeOptimizer` to *validate* (not mutate) whether a schedule is within tolerance | `iterative_optimizer.py` (`__init__`) |

When modifying shift-balance behaviour, check all three call sites above — a fix applied to only
one of them may be silently overridden or duplicated by another.

## Code style

- **Language**: Python 3.10+. The UI strings and comments are in Spanish; code identifiers are in English.
- **Imports**: Use absolute imports (`from saldo27.module import X`). Relative imports break Streamlit's script runner.
- **Type hints**: Use them on public function signatures. Use `from __future__ import annotations` if needed.
- **No classes in tests**: Write tests in functional style with plain `def test_*` functions, using pytest fixtures for setup/teardown. Never use `unittest.TestCase` or class-based test grouping.

## Testing

### Running tests

```bash
# Unit tests only (fast)
uv run pytest tests/ -m "not e2e"

# E2E tests only (starts Streamlit, needs Playwright browsers)
uv run pytest tests/e2e/ -m e2e

# Everything
uv run pytest
```

### Writing tests

- Place unit tests in `tests/test_<module>.py`.
- Place e2e tests in `tests/e2e/test_<feature>.py`.
- Mark e2e tests with `pytestmark = pytest.mark.e2e`.
- Use the shared fixtures in `tests/conftest.py` (`sample_workers_data`, `sample_schedule`, `sample_holidays`, `march_2026_dates`).
- For Playwright tests, use the `app_page` fixture from `tests/e2e/conftest.py` — it starts the Streamlit server automatically.
- Keep tests focused: one behaviour per test function.

## Development workflow

```bash
# Install all dependencies (including dev)
uv sync

# Run the app locally
uv run streamlit run src/saldo27/app_streamlit.py

# Lint and format
uv run ruff check src/ tests/        # lint (auto-fix with --fix)
uv run ruff format src/ tests/       # format

# Type check
uv run ty check                      # type checking with ty

# Check for dependency issues
uv run deptry src/                   # unused/missing dependencies

# Run tests before committing
uv run pytest tests/ -m "not e2e" -q
```

## Code quality tools

- **ruff** — linter and formatter. Config in `pyproject.toml` under `[tool.ruff]`. Run `ruff check --fix` for auto-fixes. Spanish unicode characters are intentionally allowed (RUF001/002/003 ignored).
- **ty** — type checker. Expect diagnostics on the existing codebase; focus on keeping new code clean.
- **deptry** — dependency checker. Verifies all declared deps are used and all imports are declared.

## Dependency management

- All dependencies are declared in `pyproject.toml`.
- Dev dependencies (pytest, playwright, ruff, ty, deptry) live in `[dependency-groups] dev`.
- Lock file (`uv.lock`) is committed — always run `uv sync` after pulling.
- Do not use `pip install` directly; always go through `uv`.

## Common pitfalls

- **Never use relative imports** in `src/saldo27/`. Streamlit runs files as `__main__`, so `from .module import X` will fail with `ImportError: attempted relative import with no known parent package`.
- **CWD-relative paths are intentional.** The app reads/writes data files (JSON exports, PDFs) relative to the current working directory. Do not refactor these to use `__file__`-based paths — users run the app from their data directory.
- **The `schedule` dict uses string keys** for dates (`"2026-03-15"`) and **int keys** for shift numbers (`{1: "DOC001", 2: "DOC002"}`). Some JSON round-trips convert int keys to strings — handle both.
- **Worker IDs are strings**, even when they look numeric.
- **License checks gate generation.** `license_manager.can_use()` must return `True` before calling `generate_schedule_internal`. DEMO mode limits generations (10), workers (15), and schedule length (62 days). Tests that need unlimited runs should bypass or mock `license_manager`.
- **OR-Tools is optional.** `infrastructure/optional_engines.py` wraps the import; code must degrade gracefully when `ortools` is unavailable (e.g., in constrained environments). Never import `ortools` directly at module top-level outside `optional_engines.py`.
- **Prior-schedule cross-period constraints.** When loading a previous schedule JSON via `prior_schedule_handler.py`, the extracted `prior_last_date` affects gap constraints at the period boundary. Always pass the prior handler output into `SchedulerConfig`; do not re-derive it elsewhere.
- **Bridge shifts vs. weekend shifts are separate counters.** `bridge_manager.py` maintains its own balance independently of the weekend balance in `balance_validator.py`. Modifying one does not affect the other.

## Commit conventions

- Write clear, imperative commit messages ("Add worker validation" not "Added worker validation").
- Co-author line: `Co-Authored-By: <agent name> <noreply@anthropic.com>` when AI-assisted.
- Keep PRs focused — one logical change per branch.
