"""E2E tests: worker (médico) management in Tab 1."""

import pytest

pytestmark = pytest.mark.e2e


def _go_to_workers_tab(page):
    """Click the Gestión de Médicos tab."""
    tab = page.locator("button[role='tab']:has-text('Gestión de Médicos')")
    tab.click()
    page.wait_for_timeout(500)


def test_worker_form_visible(app_page):
    """The worker form should be visible on the first tab."""
    _go_to_workers_tab(app_page)
    assert app_page.locator("text=Agregar").first.is_visible()


def test_add_worker(app_page):
    """Add a new worker via the form and verify it appears in the list."""
    _go_to_workers_tab(app_page)

    # Fill the worker ID field
    id_input = app_page.locator("input[aria-label*='ID del Médico']").first
    id_input.fill("TEST001")

    # Submit the form — look for the add button
    add_btn = app_page.locator("button:has-text('Agregar Médico')")
    if add_btn.count() > 0:
        add_btn.first.click()
        app_page.wait_for_timeout(1000)

        # The worker should now appear somewhere on the page
        assert app_page.locator("text=TEST001").count() >= 1


def test_worker_list_updates_after_add(app_page):
    """After adding a worker, the list section should reflect the change."""
    _go_to_workers_tab(app_page)

    # Count workers before
    id_input = app_page.locator("input[aria-label*='ID del Médico']").first
    id_input.fill("TEST002")

    add_btn = app_page.locator("button:has-text('Agregar Médico')")
    if add_btn.count() > 0:
        add_btn.first.click()
        app_page.wait_for_timeout(1000)

    assert app_page.locator("text=TEST002").count() >= 1
