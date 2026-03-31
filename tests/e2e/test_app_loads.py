"""E2E tests: verify the Streamlit app loads and renders correctly."""

import pytest

pytestmark = pytest.mark.e2e


def test_app_title_visible(app_page):
    """The app header should show the system title."""
    header = app_page.locator("text=Sistema de Generación de Guardias")
    assert header.first.is_visible()


def test_sidebar_configuration_visible(app_page):
    """The sidebar should show the configuration section."""
    sidebar = app_page.locator("[data-testid='stSidebar']")
    assert sidebar.is_visible()
    assert sidebar.locator("text=Configuración").first.is_visible()


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
        assert app_page.locator(f"button[role='tab']:has-text('{tab_text}')").count() >= 1, (
            f"Tab '{tab_text}' not found"
        )


def test_generate_button_in_sidebar(app_page):
    """The sidebar should have the schedule generation button."""
    sidebar = app_page.locator("[data-testid='stSidebar']")
    gen_button = sidebar.locator("text=Generar")
    assert gen_button.first.is_visible()


def test_date_inputs_in_sidebar(app_page):
    """The sidebar should contain date range configuration."""
    sidebar = app_page.locator("[data-testid='stSidebar']")
    assert sidebar.locator("text=Fecha Inicial").first.is_visible()
    assert sidebar.locator("text=Fecha Final").first.is_visible()
