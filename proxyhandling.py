import random

import numpy as np
import pymongo
from pymongo import ReplaceOne


class FailedAfterRetries(Exception):
    pass


def accepts_retries(default_retries):
    def decorator(func):

        def wrapper(*args, retries=default_retries, **kwargs):

            if retries < 0:
                raise FailedAfterRetries
            try:
                return func(*args, **kwargs)
            except pymongo.errors.AutoReconnect:
                print("pymongo error in proxy.pick: could not autoreconnect")
                return wrapper(*args, retries=retries-1, **kwargs)

        return wrapper

    return decorator


class DBProxyHandler:

    def __init__(self, db):
        self.db = db

    def upload(self, proxies):
        requests = [
            ReplaceOne({"address": item}, {"address": item, "successful_job_completion": 0}, upsert=True) for
            item in
            proxies]
        self.db.proxies.bulk_write(
            requests,
            ordered=False)

    @accepts_retries(3)
    def pick(self, n=1):
        if n < 1:
            raise ValueError("you must at least one proxy")

        proxies = self._filter_active_proxies(n)
        addresses = [proxy["address"] for proxy in proxies]
        scores = [proxy["successful_job_completion"] for proxy in proxies]

        probabilities = self._generate_probabilities(scores)
        sample_size = min(n, len(proxies))
        chosen_proxies = np.random.choice(addresses, sample_size, replace=False, p=probabilities)
        if not proxies:
            raise ValueError("no proxies available!")
        return chosen_proxies[0] if n == 1 else chosen_proxies

    def _filter_active_proxies(self, n):
        """
        Drops proxies that have not responded after 30 requests.
        """
        return list(self.db.proxies.find({"successful_job_completion": {"$gt": -30}}).limit(min(10000, 1000 * n)))

    @staticmethod
    def _generate_probabilities(scores):
        def translate_score(score):
            return min(max(score, -5), 5) + 5

        randomized_scores = [random.random() * translate_score(score) for score in scores]
        score_sum = np.sum(randomized_scores)
        return np.array(randomized_scores) / score_sum if score_sum else None

    @accepts_retries(3)
    def feedback(self, address, counter=1):
        proxy = self.db.proxies.find_one({"address": address})
        new_score = proxy.get("successful_job_completion", 0) + counter if proxy else counter
        self.db.proxies.update_one({"address": address}, {
            "$set": {"successful_job_completion": new_score}},
                                   upsert=True)
