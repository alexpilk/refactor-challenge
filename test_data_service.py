from unittest.mock import patch

import pytest

from conftest import mongopatch


@pytest.fixture
def app():
    from data_service import app
    return app


@mongopatch
def test_can_get_proxies(client, handler, mongo_client):
    with patch('data_service.get_mongo_client', lambda: mongo_client):
        uploaded_proxies = ['http://proxy1.com']
        handler.upload(uploaded_proxies)
        response = client.get('/proxies/1').json
        assert response == {
            'response': 'http://proxy1.com'
        }
