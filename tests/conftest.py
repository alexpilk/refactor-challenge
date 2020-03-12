import mongomock
import pytest

from src.proxyhandling import ProxyHandler

MONGO_LOCATION = "127.0.0.1"

mongopatch = mongomock.patch(servers=((MONGO_LOCATION, 27017),))


@pytest.fixture
def app():
    from src.data_service import app
    return app


@pytest.fixture
def mongo_client():
    client = mongomock.MongoClient(MONGO_LOCATION)
    return client


@pytest.fixture
def webdata_db(mongo_client):
    return mongo_client.webdata


@pytest.fixture
def handler(webdata_db):
    return ProxyHandler(webdata_db)
