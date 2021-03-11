from app.main import app as ticket_vault_app

import pytest


@pytest.fixture
def app():
    # Use an in-memory SQLite database so that state is wiped between tests.
    ticket_vault_app.config.DB_PATH = ":memory:"

    yield ticket_vault_app


@pytest.fixture
def test_cli(loop, app, sanic_client):
    return loop.run_until_complete(sanic_client(app))
