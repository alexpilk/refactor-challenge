import pytest

from webcacheclient import WebCacheClient


class Interceptor:

    def __init__(self, expected_request, mock_response):
        self.expected_request = expected_request
        self.mock_response = mock_response

    def signal(self, request):
        assert request == self.expected_request
        return self.mock_response


@pytest.fixture
def webcache_client():
    class MockWebCacheClient(WebCacheClient):

        def __init__(self):
            self.interceptor = None
            super(MockWebCacheClient, self).__init__()

        def intercept(self, interceptor):
            self.interceptor = interceptor

        def _get(self, url):
            return self.interceptor.signal(url)

    return MockWebCacheClient()


def test_can_get_proxies(webcache_client):
    webcache_client.intercept(
        Interceptor(
            'http://localhost:9011/proxies/2', {
                'response': ['http://proxy1.com', 'http://proxy2.com']
            }
        )
    )
    proxies = webcache_client.getProxyList(2)
    assert proxies == ['http://proxy1.com', 'http://proxy2.com']


def test_fails_if_no_proxies_are_available(webcache_client):
    webcache_client.intercept(
        Interceptor(
            'http://localhost:9011/proxies/2', None
        )
    )
    with pytest.raises(ValueError):
        webcache_client.getProxyList(2)


def test_zero_proxies(webcache_client):
    assert webcache_client.getProxyList(0) == []
