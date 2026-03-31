"""E2E tests: verify the Streamlit app loads and renders correctly."""

import pytest
from playwright.sync_api import expect

pytestmark = pytest.mark.e2e


def test_app_title_visible(app_page):
    """The app header should show the system title."""
    header = app_page.locator("text=Sistema de Generación de Guardias").first
    expect(header).to_be_visible(timeout=10000)


def test_sidebar_configuration_visible(app_page):
    """The sidebar should show the configuration section."""
    sidebar = app_page.locator("[data-testid='stSidebar']")
    expect(sidebar).to_be_visible(timeout=10000)
    expect(sidebar.locator("text=Configuración").first).to_be_visible(timeout=10000)


def test_all_tabs_present(app_page):
    """All 6 main tabs should be rendered."""
    expected_tabs = [
        "Gestión de Médicos",
        "Calendario Generado",
        "Estadísticas",
        "Verificación",
        "Predictive",
        "Revisión",
    ]
    for tab_text in expected_tabs:
        tab = app_page.locator(f"button[role='tab']:has-text('{tab_text}')")
        expect(tab.first).to_be_visible(timeout=10000)


def test_generate_button_in_sidebar(app_page):
    """The sidebar should have the schedule generation button."""
    sidebar = app_page.locator("[data-testid='stSidebar']")
    gen_button = sidebar.locator("text=Generar").first
    expect(gen_button).to_be_visible(timeout=10000)


def test_date_inputs_in_sidebar(app_page):
    """The sidebar should contain date range configuration."""
    sidebar = app_page.locator("[data-testid='stSidebar']")
    expect(sidebar.locator("text=Fecha Inicial").first).to_be_visible(timeout=10000)
    expect(sidebar.locator("text=Fecha Final").first).to_be_visible(timeout=10000)
