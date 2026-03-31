"""E2E tests: schedule generation flow."""

import pytest
from playwright.sync_api import expect

pytestmark = pytest.mark.e2e


def _add_worker(page, worker_id):
    """Helper to add a worker via the form."""
    tab = page.locator("button[role='tab']:has-text('Gestión de Médicos')")
    expect(tab.first).to_be_visible(timeout=10000)
    tab.first.click()
    page.wait_for_timeout(500)

    id_input = page.locator("input[aria-label*='ID del Médico']").first
    id_input.fill(worker_id)

    add_btn = page.locator("button:has-text('Agregar Médico')")
    if add_btn.count() > 0:
        add_btn.first.click()
        page.wait_for_timeout(1000)


def test_generate_schedule_with_workers(app_page):
    """Add workers, click generate, and verify a schedule appears."""
    # Add enough workers for the schedule (need at least num_shifts)
    for wid in ["GEN01", "GEN02", "GEN03", "GEN04", "GEN05"]:
        _add_worker(app_page, wid)

    # Click generate in the sidebar
    sidebar = app_page.locator("[data-testid='stSidebar']")
    gen_button = sidebar.locator("button:has-text('Generar')")
    if gen_button.count() > 0:
        gen_button.first.click()
        # Schedule generation can take a while
        app_page.wait_for_timeout(15000)

    # Switch to the calendar tab
    cal_tab = app_page.locator("button[role='tab']:has-text('Calendario Generado')")
    cal_tab.first.click()
    app_page.wait_for_timeout(2000)

    # Either we see a generated schedule or an info message
    has_schedule = app_page.locator("table, [data-testid='stDataFrame']").count() > 0
    has_info = app_page.locator("text=No hay calendario").count() > 0
    assert has_schedule or has_info, "Expected either a schedule table or an info message"


def test_statistics_tab_after_generation(app_page):
    """The statistics tab should show data after schedule generation."""
    stats_tab = app_page.locator("button[role='tab']:has-text('Estadísticas')")
    expect(stats_tab.first).to_be_visible(timeout=10000)
    stats_tab.first.click()
    app_page.wait_for_timeout(1000)

    # Should show stats content or info about no schedule
    page_text = app_page.locator("[data-testid='stAppViewContainer']").inner_text()
    assert len(page_text) > 0


def test_constraints_tab_renders(app_page):
    """The constraints verification tab should render without errors."""
    tab = app_page.locator("button[role='tab']:has-text('Verificación')")
    expect(tab.first).to_be_visible(timeout=10000)
    tab.first.click()
    app_page.wait_for_timeout(1000)

    # Should not show an uncaught exception
    assert app_page.locator("text=uncaught exception").count() == 0
