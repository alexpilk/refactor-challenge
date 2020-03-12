import mongomock
import pytest

from proxyhandling import DBProxyHandler

MONGO_LOCATION = "127.0.0.1"

mongopatch = mongomock.patch(servers=((MONGO_LOCATION, 27017),))


@pytest.fixture
def mongo_client():
    client = mongomock.MongoClient(MONGO_LOCATION)
    return client


@pytest.fixture
def db(mongo_client):
    return mongo_client.webdata


@pytest.fixture
def handler(db):
    return DBProxyHandler(db)
