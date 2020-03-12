from unittest.mock import patch

import pymongo
import pytest

from conftest import mongopatch
from proxyhandling import FailedAfterRetries, ProxyError


@mongopatch
def test_fails_when_no_proxies_available(handler):
    with pytest.raises(ProxyError):
        handler.pick(1)


@mongopatch
def test_can_pick_one_proxy(handler):
    uploaded_proxies = ['http://proxy1.com', 'http://proxy2.com']
    handler.upload(uploaded_proxies)
    fetched_proxy = handler.pick(1)
    assert fetched_proxy in uploaded_proxies


@mongopatch
def test_can_pick_multiple_proxies(handler):
    uploaded_proxies = ['http://proxy1.com', 'http://proxy2.com']
    handler.upload(uploaded_proxies)
    fetched_proxies = handler.pick(2)
    assert set(uploaded_proxies) == set(fetched_proxies)


@mongopatch
@pytest.mark.parametrize('number_of_proxies', (0, -5))
def test_cannot_pick_less_than_one_proxy(handler, number_of_proxies):
    with pytest.raises(ProxyError):
        handler.pick(number_of_proxies)


def sort_by_score(proxies, n, replace, p):
    """
    Replaces numpy.random.choice with a deterministic function. All arguments are identical.
    """
    if p is None:
        return proxies[:n]
    return [zipped[0] for zipped in sorted(zip(proxies, p), key=lambda zipped: 1 - zipped[1])][:n]


@patch('numpy.random.choice', sort_by_score)
@patch('random.random', lambda: 1.0)
@mongopatch
def test_better_proxies_have_higher_priorities(handler):
    uploaded_proxies = ['http://proxy1.com', 'http://proxy2.com']
    handler.upload(uploaded_proxies)
    handler.feedback('http://proxy2.com', 1)
    fetched_proxies = handler.pick(2)
    assert fetched_proxies == ['http://proxy2.com', 'http://proxy1.com']


@patch('numpy.random.choice', sort_by_score)
@patch('random.random', lambda: 1.0)
@mongopatch
def test_worse_proxies_have_lower_priorities(handler):
    uploaded_proxies = ['http://proxy1.com', 'http://proxy2.com']
    handler.upload(uploaded_proxies)
    handler.feedback('http://proxy1.com', -25)
    fetched_proxies = handler.pick(2)
    assert fetched_proxies == ['http://proxy2.com', 'http://proxy1.com']


@mongopatch
def test_proxy_is_dropped_after_too_many_failures(handler):
    uploaded_proxies = ['http://proxy1.com', 'http://proxy2.com']
    handler.upload(uploaded_proxies)
    handler.feedback('http://proxy2.com', -30)
    fetched_proxies = handler.pick(2)
    assert fetched_proxies == ['http://proxy1.com']


def raise_reconnect_exception(*args, **kwargs):
    raise pymongo.errors.AutoReconnect()


def test_feedback_fails_after_retries(handler):
    handler.db.proxies.find_one = raise_reconnect_exception
    with pytest.raises(FailedAfterRetries):
        handler.feedback('whatever', 1)


def test_pick_fails_after_retries(handler):
    handler.db.proxies.find = raise_reconnect_exception
    with pytest.raises(FailedAfterRetries):
        handler.pick(1)


@mongopatch
def test_can_pick_proxy_after_retries(handler):
    uploaded_proxies = ['http://proxy1.com', 'http://proxy2.com']
    handler.upload(uploaded_proxies)

    has_failed = False
    find_operation = handler.db.proxies.find

    def fail_once(*args, **kwargs):
        """
        Fails once, then works after first retry.
        """
        nonlocal has_failed
        if has_failed:
            return find_operation(*args, **kwargs)
        else:
            has_failed = True
            raise_reconnect_exception()

    handler.db.proxies.find = fail_once
    fetched_proxy = handler.pick(1)
    assert fetched_proxy in uploaded_proxies
