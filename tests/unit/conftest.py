import pathlib
import tempfile

from app.main import app as ticket_vault_app

import pytest


@pytest.fixture
def in_memory_app():
    # Use an in-memory SQLite database so that state is wiped between tests.
    ticket_vault_app.config.DB_PATH = ":memory:"

    yield ticket_vault_app


@pytest.fixture(scope="module")
def persistent_app():
    with tempfile.TemporaryDirectory() as db_dir:
        ticket_vault_app.config.DB_PATH = str(pathlib.Path(db_dir) / "tickets.db")
        yield ticket_vault_app


@pytest.fixture
def test_cli(loop, in_memory_app, sanic_client):
    return loop.run_until_complete(sanic_client(in_memory_app))


@pytest.fixture
def test_persistent_cli(loop, persistent_app, sanic_client):
    return loop.run_until_complete(sanic_client(persistent_app))
