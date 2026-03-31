# Saldo27 — Agent Directives

Shared guidelines for AI coding assistants (Claude, Copilot, etc.) working on this project.

## Project overview

Saldo27 is a **worker shift scheduling system** for medical staff, built with Streamlit. It generates optimised monthly schedules respecting constraints like minimum gaps between shifts, maximum consecutive weekends, incompatibilities between workers, and mandatory/off days.

## Architecture

```
src/saldo27/          # All source code — installable Python package
tests/                # pytest test suite (unit + e2e)
  e2e/                # Playwright browser tests against the Streamlit app
docs/                 # Project documentation
packaging/            # PyInstaller hooks, .spec file, Windows installer
```

### Key modules

| Module | Role |
|--------|------|
| `app_streamlit.py` | Streamlit UI — the main entry point |
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

## Commit conventions

- Write clear, imperative commit messages ("Add worker validation" not "Added worker validation").
- Co-author line: `Co-Authored-By: <agent name> <noreply@anthropic.com>` when AI-assisted.
- Keep PRs focused — one logical change per branch.
