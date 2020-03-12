import mongomock
import pymongo
import pytest

from proxyhandling import DBProxyHandler

MONGO_LOCATION = "127.0.0.1"


@pytest.fixture
def db():
    client = pymongo.MongoClient(MONGO_LOCATION)
    db = client.webdata
    return db


@pytest.fixture
def handler(db):
    return DBProxyHandler(db)


@mongomock.patch(servers=((MONGO_LOCATION, 27017),))
def test_proxies_are_added(handler):
    uploaded_proxies = ['http://proxy1.com', 'http://proxy2.com']
    handler.upload(uploaded_proxies)
    fetched_proxies = handler.pick(2)
    assert set(uploaded_proxies) == set(fetched_proxies)
